//! 协调 Antigravity 账号选择、并发排队和租约，取得租约后复核凭据

use super::{
    AntigravityProvider,
    candidates::{CandidatePool, Project},
};
use crate::{credential::Material, transport::unsent};
use gateway_core::{
    account::{AccountCapacitySnapshot, AccountSelector, ProviderAccount},
    concurrency::{CapacityWait, ConcurrencyQueuePolicy, QueueRejection},
    engine::{
        AttemptContext,
        policy::{AccountPolicyError, AccountScheduleOwnership},
    },
    error::{ProviderDiagnostic, ProviderError, ProviderErrorKind as Kind},
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeaseGuard, ProviderLeaseRequest,
        ProviderSchedulingLeaseRequest,
    },
    routing::UpstreamModelId,
};
use std::time::Duration;

pub(super) struct SelectedAccount {
    pub(super) account: ProviderAccount,
    pub(super) capacity: Option<AccountCapacitySnapshot>,
    pub(super) material: Material,
    pub(super) project: Project,
    pub(super) lease: Box<dyn ProviderLeaseGuard>,
}

#[derive(Debug, thiserror::Error)]
enum SelectionError {
    #[error(transparent)]
    Queue(#[from] QueueRejection),
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

impl AntigravityProvider {
    pub(super) async fn select_account(
        &self,
        model: &UpstreamModelId,
        ctx: &AttemptContext,
    ) -> Result<SelectedAccount, ProviderError> {
        let policy = ctx.account_selection_policy();
        let select = |queue| self.select_with_queue(model, ctx, queue);
        let result = match select(policy.queue_policy()).await {
            Err(SelectionError::Queue(reason)) if ctx.required_account().is_none() => {
                policy
                    .reselect_after_queue_rejection(
                        reason,
                        ctx.deadline().remaining(),
                        select,
                        |error| match error {
                            SelectionError::Provider(error) => matches!(
                                error.kind(),
                                Kind::NoEligibleAccount | Kind::AccountCapacityUnavailable
                            ),
                            SelectionError::Queue(_) => false,
                        },
                    )
                    .await
            }
            result => result,
        };
        result.map_err(|error| match error {
            SelectionError::Queue(reason) => unsent(reason.provider_kind()),
            SelectionError::Provider(error) => error,
        })
    }

    async fn select_with_queue(
        &self,
        model: &UpstreamModelId,
        ctx: &AttemptContext,
        queue: ConcurrencyQueuePolicy,
    ) -> Result<SelectedAccount, SelectionError> {
        let mut waiting = CapacityWait::new(
            &self.waiting,
            queue,
            ctx.deadline().at(),
            ctx.concurrency_wait_budget(),
        )
        .with_periodic_recheck(ctx.required_account().is_none());
        'refresh: loop {
            if ctx.cancellation().is_cancelled() {
                return Err(unsent(Kind::Cancelled).into());
            }
            if ctx.deadline().is_elapsed() {
                return Err(unsent(Kind::Timeout).into());
            }
            let CandidatePool {
                accounts,
                mut projects,
                mut selection,
            } = self.load_candidates(model, ctx).await?;
            for candidate in &accounts {
                if !waiting.can_try(candidate.account.id()) {
                    selection
                        .busy_accounts
                        .insert(candidate.account.id().clone());
                }
            }
            let excluded = std::mem::take(&mut selection.excluded_accounts);
            let mut retry_after: Option<Duration> = None;
            loop {
                // 每次策略求值都从相同的授权、冷却和重试排除集合开始
                selection.excluded_accounts.clone_from(&excluded);
                let selected = ctx
                    .select_account(
                        &self.kind,
                        Some(model.as_str()),
                        &accounts,
                        &mut selection,
                        AccountScheduleOwnership {
                            pinned_account: ctx.required_account(),
                            ..Default::default()
                        },
                    )
                    .await;
                let candidate = match selected {
                    Ok(Some(selected)) => selected.candidate(),
                    Ok(None) => break,
                    Err(AccountPolicyError::StaleCandidate) => continue 'refresh,
                    Err(AccountPolicyError::Filtered) => {
                        return Err(unsent(Kind::RequestPolicyDenied)
                            .with_diagnostic(
                                ProviderDiagnostic::new(AccountPolicyError::Filtered.to_string())
                                    .with_classification(
                                        "account_selection",
                                        "account_policy_filtered",
                                    ),
                            )
                            .into());
                    }
                    Err(AccountPolicyError::Rejected) => {
                        return Err(unsent(Kind::RequestPolicyDenied).into());
                    }
                    Err(AccountPolicyError::Fault) => {
                        return Err(unsent(Kind::ProviderInfrastructureUnavailable).into());
                    }
                };
                let account = &candidate.account;
                let acquire = ProviderSchedulingLeaseRequest::new(
                    self.kind.clone(),
                    account.id().clone(),
                    account.revision(),
                    selection.concurrency_limit(candidate),
                    ctx.deadline(),
                )
                .with_metadata(ctx.lease_metadata())
                .with_cancellation(ctx.cancellation().clone());
                match self
                    .leases
                    .try_acquire(ProviderLeaseRequest::Scheduling(acquire))
                    .await
                    .map_err(super::unavailable)?
                {
                    ProviderLeaseAcquisition::Acquired(lease) => {
                        let (_, project) = projects
                            .remove(account.id())
                            .ok_or_else(|| unsent(Kind::NoEligibleAccount))?;
                        let capacity = AccountSelector
                            .capacity_snapshot(&accounts, &selection)
                            .map(AccountCapacitySnapshot::with_acquired_request);
                        return Ok(self
                            .load_selected(account, project, lease, capacity, ctx)
                            .await?);
                    }
                    ProviderLeaseAcquisition::Busy { retry_after: delay } => {
                        if let Some(delay) = delay {
                            retry_after =
                                Some(retry_after.map_or(delay, |current| current.min(delay)));
                        }
                        selection.busy_accounts.insert(account.id().clone());
                    }
                }
            }
            let keys = AccountSelector.wait_candidates(&accounts, &selection);
            if keys.is_empty() {
                return Err(unsent(Kind::NoEligibleAccount).into());
            }
            if queue.max_waiting == 0 || ctx.is_diagnostic_required_account() {
                let mut error = unsent(Kind::AccountCapacityUnavailable);
                if let Some(delay) = retry_after {
                    error = error.with_retry_after(delay);
                }
                return Err(error.into());
            }
            tokio::select! {
                biased;
                () = ctx.cancellation().cancelled() => return Err(unsent(Kind::Cancelled).into()),
                result = AccountSelector.wait_for_capacity(&mut waiting, &keys, &accounts, &selection) => result?,
            }
        }
    }

    async fn load_selected(
        &self,
        account: &ProviderAccount,
        project: Project,
        lease: Box<dyn ProviderLeaseGuard>,
        capacity: Option<AccountCapacitySnapshot>,
        ctx: &AttemptContext,
    ) -> Result<SelectedAccount, ProviderError> {
        let current = self
            .credentials
            .current(account.id())
            .await
            .map_err(super::unavailable)?;
        if current.account.revision() != account.revision()
            || ctx.excludes_proxy(current.account.outbound_proxy())
        {
            return Err(unsent(Kind::NoEligibleAccount));
        }
        let material = self.credentials.material(&current)?;
        Ok(SelectedAccount {
            account: current.account,
            capacity,
            material,
            project,
            lease,
        })
    }
}
