# KKBOX validation results

These are validated results for the current KKBOX snapshot used in this project.
They are evidence, **not permanent dbt test constants**: refreshed source data
may legitimately change them. Recompile and rerun the read-only SQL in
`analysis/validation/` after material source or model changes, then record the
new snapshot and execution date. The execution date for the results below was
not supplied with this snapshot.

Compile the analyses with the configured dbt profile, then execute the compiled
SQL in BigQuery. These queries read already-built models; they do not rebuild
models or write tables. The daily-log diagnostic scans the large staging model.
Use `dbt compile --profiles-dir .` for the repo-local profile or `dbt compile`
for `~/.dbt/profiles.yml`.

The Phase 2 dbt documentation and tests passed in Cloud Shell with dbt Core
1.12.5 and `dbt-bigquery` 1.12.1. These Phase 3 validation queries are
separate from production dbt tests. `user_log_customer_day_diagnostic.sql` is
an expensive manual query over the daily staging table; run it deliberately.

## Customer-month spine

| Measure | Validated result |
| --- | ---: |
| `mart_customer_month` rows | 94,385,637 |
| Distinct customers | 5,827,334 |
| Engagement customer-months | 27,862,865 |
| Zero-engagement customer-months | 66,522,772 |
| Transaction customer-months | 22,026,504 |

Engagement and zero-engagement months sum to the mart row count. A zero-engagement
month is inside the modeled observation window and has zero additive listening
and play totals; an unobservable month is not a customer-month row. Invalid
listening measurements instead retain activity presence and have NULL listening
metrics.

## Subscription lifecycle

| Measure | Validated result |
| --- | ---: |
| Original transaction records | 22,978,755 |
| Ordered lifecycle event groups | 22,975,295 |
| Renewal events | 15,179,143 |
| Late-renewal events | 3,804,843 |
| Reactivation events | 690,095 |
| Cancellation events | 891,301 |

Transaction records and ordered event groups are different grains. Ordering-key
ties do not establish duplicate financial transactions. Cancellation is an
observed event, not an automatic churn label; first observed subscription is
not necessarily true acquisition. No present-day active/churned status is
inferred.

## Engagement quality

| Measure | Validated result |
| --- | ---: |
| Engagement customer-months | 27,862,865 |
| Valid listening months | 27,732,473 |
| Invalid listening months | 130,392 |
| Invalid listening days | 208,686 |
| Invalid play months | 0 |
| Invalid daily unique-song months | 0 |
| Invalid listening months with populated listening metrics | 0 |
| Valid months with average listening above 86,400 seconds per active day | 0 |

A valid daily listening value is from 0 through 86,400 seconds inclusive. One
invalid contributing day makes that month's listening metrics NULL without
removing its observed activity. Source `num_unq` means unique songs within a
day, not distinct songs across the month.

## Event-relative mart and cohort coverage

| Measure | Validated result |
| --- | ---: |
| `mart_lifecycle_event_engagement` rows | 21,464,268 |
| Unique event-relative-month keys | 21,464,268 |
| Duplicate event-relative-month rows | 0 |

| Event type | Event customer-months | Underlying events | Balanced event customer-months |
| --- | ---: | ---: | ---: |
| Cancellation | 886,480 | 891,301 | 533,576 |
| Late renewal | 3,789,720 | 3,804,843 | 3,351,531 |
| Reactivation | 689,867 | 690,095 | 658,885 |

Multiple events of one type can occur in the same customer-month. `event_count`
is repeated on its four relative-month rows, so it must be counted once per
event customer-month when reconciling to underlying lifecycle events.
`balanced_panel_flag` identifies event customer-months with all four relative
months (-3, -2, -1, 0) inside the observable customer-month spine. It is a
cohort-quality control for like-for-like trajectories, not a requirement for
engagement in every month.

## Balanced-panel interpretation

- **Cancellation:** The unrestricted cohort appeared to deteriorate strongly
  before cancellation. With the same balanced event cohort at -3, -2, and -1,
  behavior is comparatively flat. Changing history coverage and left-censoring
  drove most of the earlier apparent decline. Month 0 drops, but it includes
  behavior both before and after the actual cancellation date.
- **Late renewal:** Engagement remains strong approaching the event. Late
  renewal is not interchangeable with churn or disengagement.
- **Reactivation:** Engagement declines from -3 to -1 and rebounds strongly in
  month 0. This is an observed association, not a causal claim; month 0 also
  mixes behavior before and after the event.

The balanced panel addresses the modeled window's left-censoring for this
comparison. It does not recover unobserved history before the source coverage
or establish causal effects. Source-recorded payment amounts are not labeled
revenue or LTV.

Trajectory averages ignore NULL measurements. Observable zero-engagement
months contribute zero to monthly totals and active-day counts, while averages
that require active days or a nonzero play denominator remain NULL for them.
