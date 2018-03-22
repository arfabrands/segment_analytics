connection: "data_warehouse"

# include all views in this project
# - include: "*.dashboard.lookml"  # include all dashboards in this project
include: "*.view"

# - explore: pages

explore: event_facts {
  view_label: "Events"
  label: "Events-Pages"

  join: pages {
    view_label: "Events"
    type: left_outer
    sql_on: event_facts.id = pages.id
      and event_facts.received_at = pages.received_at
      and event_facts.anonymous_id = pages.anonymous_id
      and pages.received_at >= now() - interval '3 months'
       ;;
    relationship: one_to_one
  }

  join: tracks {
    view_label: "Events"
    type: left_outer
    sql_on: event_facts.event_id = concat(tracks.event_id, '-t')
      and event_facts.received_at = tracks.received_at
      and event_facts.anonymous_id = tracks.anonymous_id
      and tracks.received_at >= now() - interval '3 months'
       ;;
    relationship: one_to_one
    fields: []
  }

  join: page_facts {
    view_label: "Events"
    type: left_outer
    sql_on: event_facts.event_id = page_facts.event_id and
      event_facts.received_at = page_facts.received_at and
      event_facts.looker_visitor_id = page_facts.looker_visitor_id
       ;;
    relationship: one_to_one
  }

  join: sessions_pg_trk {
    view_label: "Sessions"
    type: left_outer
    sql_on: ${event_facts.session_id} = ${sessions_pg_trk.session_id} ;;
    relationship: many_to_one
  }

  join: session_pg_trk_facts {
    view_label: "Sessions"
    type: left_outer
    sql_on: ${event_facts.session_id} = ${session_pg_trk_facts.session_id} ;;
    relationship: many_to_one
  }
}