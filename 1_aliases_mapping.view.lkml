view: aliases_mapping {
  derived_table: {
    sql_trigger_value: select current_date ;;
    indexes: ["looker_visitor_id", "alias"]
    sql: with
      all_mappings as (
        select anonymous_id
        , user_id
        , received_at as received_at
        from lieudit_com_production.tracks
        where received_at >= now() - interval '3 months'

        union

        select user_id
          , null
          , received_at
        from lieudit_com_production.tracks
        where received_at >= now() - interval '3 months'
      )

      select
        distinct anonymous_id as alias
        , first_value(user_id) OVER ()

        , coalesce(first_value(user_id)
        over(
          partition by anonymous_id
          order by case when user_id is not null then 0 else 1 end,received_at
          rows between unbounded preceding and unbounded following),anonymous_id) as looker_visitor_id
      from all_mappings
       ;;
  }

  # Anonymous ID
  dimension: alias {
    primary_key: yes
    type: string
    sql: ${TABLE}.alias ;;
  }

  # User ID
  dimension: looker_visitor_id {
    type: string
    sql: ${TABLE}.looker_visitor_id ;;
  }
}
