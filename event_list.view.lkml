# Derived Table of Event Names used for Filter Suggestions

view: event_list {
  derived_table: {
    sql_trigger_value: SELECT FLOOR((EXTRACT(EPOCH FROM now() AT TIME ZONE 'US/Pacific') - 60*60*2)/(60*60*24)) ;;
    indexes: ["event_types"]
    sql: SELECT
        event as event_types
      FROM goodee_shopify.tracks
      where received_at >= now() - interval '3 months'
      GROUP BY 1
       ;;
  }

  dimension: event_types {
    type: string
    sql: ${TABLE}.event_types ;;
  }
}
