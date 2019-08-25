## Intermediate Table

view: mapped_tracks {
  derived_table: {
    indexes: ["event_id","looker_visitor_id"]
    sql_trigger_value: select current_date ;;
    sql: select *
         , date_part('minute',received_at- lag(received_at) over(partition by looker_visitor_id order by received_at)) as idle_time_minutes
        from (
          select CONCAT(t.received_at, t.id) as event_id
          , t.anonymous_id
          , a2v.looker_visitor_id
          , t.received_at
          , t.event as event
          , t.uuid
          from liedit_com_production.tracks as t
          inner join ${aliases_mapping.SQL_TABLE_NAME} as a2v
          on a2v.alias = coalesce(t.user_id, t.anonymous_id)
          where t.received_at >= now() - interval '3 months'
        ) tt
       ;;
  }

  dimension: anonymous_id {
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: uuid {
    sql: ${TABLE}.id ;;
  }

  dimension: event_id {
    sql: ${TABLE}.event_id ;;
  }

  dimension: looker_visitor_id {
    sql: ${TABLE}.looker_visitor_id ;;
  }

  dimension_group: received_at {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.received_at ;;
  }

  dimension: event {
    sql: ${TABLE}.event ;;
  }

  dimension: idle_time_minutes {
    type: number
    sql: ${TABLE}.idle_time_minutes ;;
  }

  set: detail {
    fields: [event_id, looker_visitor_id, received_at_date, event, idle_time_minutes]
  }
}
