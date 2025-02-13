# Determines event sequence numbers within session

view: track_facts {
  derived_table: {
    sql_trigger_value: select count(*) from ${sessions_trk.SQL_TABLE_NAME} ;;
    indexes: ["event_id","looker_visitor_id","id"]
    sql: select t.anonymous_id
          , t.received_at
          , t.event_id
          , t.id
          , t.event
          , s.session_id
          , t.looker_visitor_id
          , row_number() over(partition by s.session_id order by t.received_at) as track_sequence_number
        from ${mapped_tracks.SQL_TABLE_NAME} as t
        inner join ${sessions_trk.SQL_TABLE_NAME} as s
        on t.looker_visitor_id = s.looker_visitor_id
          and t.received_at >= s.session_start_at
          and (t.received_at < s.next_session_start_at or s.next_session_start_at is null)
       ;;
  }

  dimension: event_id {
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.event_id ;;
  }

  dimension: event {
    hidden: yes
    sql: ${TABLE}.event ;;
  }

  dimension: uuid {
    hidden: yes
    sql: ${TABLE}.id ;;
  }

  dimension: session_id {
    sql: ${TABLE}.session_id ;;
  }

  dimension: looker_visitor_id {
    sql: ${TABLE}.looker_visitor_id ;;
  }

  dimension: sequence_number {
    type: number
    sql: ${TABLE}.track_sequence_number ;;
  }

  measure: count_visitors {
    type: count_distinct
    sql: ${looker_visitor_id} ;;
  }
}
