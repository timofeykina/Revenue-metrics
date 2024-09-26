with monthly_revenue as (
    select  
        gp.user_id,
        gp.game_name,
        date_trunc('month', gp.payment_date) as payment_month,
        sum(gp.revenue_amount_usd) as total_revenue
    from 
        project.games_payments gp
    group by
        gp.user_id, gp.game_name, payment_month
),
revenue_lag_lead_months as (
    select
        *,
        date(payment_month - interval '1' month) as previous_calendar_month,
        date(payment_month + interval '1' month) as next_calendar_month,
        lag(total_revenue) over(partition by user_id order by payment_month) as previous_paid_month_revenue,
        lag(payment_month) over(partition by user_id order by payment_month) as previous_paid_month,
        lead(payment_month) over(partition by user_id order by payment_month) as next_paid_month 
    from monthly_revenue
),
revenue_metrics as (
    select 
        payment_month,
        user_id,
        game_name,
        total_revenue,
        case
            when previous_paid_month is null
            then total_revenue
        end as new_mrr,
        case 
            when previous_paid_month = previous_calendar_month
            and total_revenue > previous_paid_month_revenue
            then total_revenue - previous_paid_month_revenue
        end as expansion_revenue,
        case 
            when previous_paid_month = previous_calendar_month
            and total_revenue < previous_paid_month_revenue
            then previous_paid_month_revenue - total_revenue
        end as contraction_revenue,
        case 
            when previous_paid_month != previous_calendar_month
            and previous_paid_month is not null
            then total_revenue
        end as back_from_churn_revenue,
        case
            when next_paid_month is null 
            or next_paid_month != next_calendar_month
            then total_revenue
        end as churned_revenue,
        case
            when next_paid_month is null 
            or next_paid_month != next_calendar_month
            then next_calendar_month
        end as churn_month
    from revenue_lag_lead_months 
),
monthly_metrics as (
    select
        rm.payment_month,
        count(distinct rm.user_id) as paid_users,
        sum(rm.total_revenue) as mrr,
        avg(rm.total_revenue) as arppu,
        count(distinct case when rm.new_mrr is not null then rm.user_id end) as new_paid_users,
        sum(case when rm.new_mrr is not null then rm.new_mrr else 0 end) as new_mrr,
        sum(case when rm.churned_revenue is not null then rm.churned_revenue else 0 end) as churned_revenue,
        sum(case when rm.expansion_revenue is not null then rm.expansion_revenue else 0 end) as expansion_mrr,
        sum(case when rm.contraction_revenue is not null then rm.contraction_revenue else 0 end) as contraction_mrr
    from 
        revenue_metrics rm
    group by 
        rm.payment_month
),
churn_rate_metrics as (
    select
        rm.payment_month,
        count(distinct rm.user_id) as churned_users,
        coalesce(sum(rm.churned_revenue), 0) as churned_revenue,
        coalesce(count(distinct rm.user_id)::numeric / lag(count(distinct rm.user_id)) over (order by rm.payment_month), 0) as churn_rate,
        coalesce(sum(rm.churned_revenue) / lag(sum(rm.total_revenue)) over (order by rm.payment_month), 0) as revenue_churn_rate
    from 
        revenue_metrics rm
    where 
        rm.churn_month is not null
    group by 
        rm.payment_month
),
customer_lifetime as (
    select
        gpu.user_id,
        date_part('year', max(gp.payment_date)) * 12 + date_part('month', max(gp.payment_date)) -
        (date_part('year', min(gp.payment_date)) * 12 + date_part('month', min(gp.payment_date))) as lifetime_months
    from 
        project.games_paid_users gpu
    join 
        project.games_payments gp on gpu.user_id = gp.user_id
    group by 
        gpu.user_id
),
customer_lifetime_value as (
    select
        gpu.user_id,
        sum(gp.revenue_amount_usd) as ltv
    from 
        project.games_paid_users gpu
    join 
        project.games_payments gp on gpu.user_id = gp.user_id
    group by 
        gpu.user_id
),
avg_metrics as (
    select
        avg(lifetime_months) as avg_lifetime_months
    from 
        customer_lifetime
),
avg_ltv as (
    select
        avg(ltv) as avg_ltv
    from 
        customer_lifetime_value
)

select 
    rm.*,
    gpu.language,
    gpu.has_older_device_model,
    gpu.age,
    mm.paid_users,
    mm.mrr,
    mm.arppu,
    mm.new_paid_users,
    mm.new_mrr,
    mm.expansion_mrr,
    mm.contraction_mrr,
    cm.churned_users,
    cm.churn_rate,
    cm.churned_revenue,
    cm.revenue_churn_rate,
    am.avg_lifetime_months as avg_lifetime,
    alt.avg_ltv
from 
    revenue_metrics rm
left join 
    project.games_paid_users as gpu using(user_id)
left join 
    monthly_metrics mm on rm.payment_month = mm.payment_month
left join 
    churn_rate_metrics cm on rm.payment_month = cm.payment_month
left join 
    avg_metrics am on true
left join 
    avg_ltv alt on true;
