library(arrow)
library(dplyr)
library(jsonlite)

# ---- Port definitions ----
ports <- tibble::tribble(
  ~name,               ~lat,    ~lon,    ~radius_km,
  "Hobart",           -42.88,  147.33,   15,
  "Burnie",           -41.05,  145.91,   8,
  "Macquarie Island", -54.50,  158.94,   40,
  "Heard Island",     -53.10,   73.51,   50,
  "Casey",            -66.28,  110.53,   80,
  "Davis",            -68.58,   77.97,   80,
  "Mawson",           -67.60,   62.87,   80
)

# ---- Haversine ----
haversine_km <- function(lat1, lon1, lat2, lon2) {
  R <- 6371
  toRad <- pi / 180
  dLat <- (lat2 - lat1) * toRad
  dLon <- (lon2 - lon1) * toRad
  a <- sin(dLat/2)^2 + cos(lat1 * toRad) * cos(lat2 * toRad) * sin(dLon/2)^2
  R * 2 * atan2(sqrt(a), sqrt(1 - a))
}

# ---- Detect nearest port if within radius ----
detect_port <- function(lat, lon, ports) {
  dists <- haversine_km(lat, lon, ports$lat, ports$lon)
  idx <- which.min(dists)
  if (dists[idx] <= ports$radius_km[idx]) ports$name[idx] else NA_character_
}

# ---- Main ----
d <- arrow::read_parquet("https://github.com/mdsumner/uwy.new/releases/download/v0.0.1/nuyina_underway.parquet") |>
  arrange(datetime)

# tag each point with port (or NA if at sea)
d <- d |>
  rowwise() |>
  mutate(port = detect_port(latitude, longitude, ports)) |>
  ungroup()

# find port visits: runs of consecutive port presence
d <- d |>
  mutate(
    port_change = port != lag(port) | is.na(lag(port)),
    visit_id = cumsum(port_change & !is.na(port))
  )

# summarise visits
visits <- d |>
  filter(!is.na(port)) |>
  group_by(visit_id, port) |>
  summarise(
    arrive = min(datetime),
    depart = max(datetime),
    arrive_gml_id = gml_id[which.min(datetime)],
    depart_gml_id = gml_id[which.max(datetime)],
    dwell_hours = as.numeric(difftime(max(datetime), min(datetime), units = "hours")),
    n_points = n(),
    .groups = "drop"
  ) |>
  filter(dwell_hours >= 2) |>
  arrange(arrive
# split into voyages (Hobart departures)
visits <- visits |>
  mutate(voyage_break = port == "Hobart" & lag(port, default = "Hobart") != "Hobart") |>
  mutate(voyage_id = cumsum(voyage_break | row_number() == 1))

voyages <- visits |>
  group_by(voyage_id) |>
  summarise(
    start = min(arrive),
    end = max(depart),
    stops = list(tibble(port, arrive, depart, dwell_hours)),
    .groups = "drop"
  ) |>
  mutate(
    id = sprintf("V%d %s", row_number(), format(start, "%Y-%m")),
    note = ""
  )

# ---- Output JSON ----
ports_list <- ports |>
  split(seq_len(nrow(ports))) |>
  lapply(as.list) |>
  setNames(ports$name)

voyages_list <- voyages |>
  rowwise() |>
  mutate(
    stops = list(stops |> purrr::transpose())
  ) |>
  ungroup() |>
  select(id, note, start, end, stops) |>
  purrr::transpose()

output <- list(
  `_generated` = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
  `_note` = "Auto-detected draft - review and edit before publishing",
  ports = ports_list,
  voyages = voyages_list
)

#jsonlite::write_json(output, "voyages_draft.json", pretty = TRUE, auto_unbox = TRUE)
