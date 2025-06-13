
#' Obtain and save the Nuyina underway
#'
#' Data is read from the AADC geoserver feed.
#'
#' We take a rough offset from the existing data and merge, it might make the query faster.
#'
#' We convert 'datetime' to POSIXct here.
#'
#' We apply a weird fix to longitudes if they are negative for a bug that appeared in October 2023.
#'
#' @param init update existing data or initialize it (FALSE by default, data is appended)
#' @param filename name of file to create (or use default)
#'
#' @return the data
#' @export
get_underway <- function(init = FALSE, filename = NULL) {

  filename <- "nuyina_underway.parquet"
  #piggyback::pb_download(filename, "mdsumner/uwy.new", tag = "v0.0.1")
  if (file.exists(filename)) file.remove(filename)
  curl::curl_download("https://github.com/mdsumner/uwy.new/releases/download/v0.0.1/nuyina_underway.parquet", filename)
  dat <- NULL
  offset <- 0
  query0 <- "SELECT * FROM \"underway:nuyina_underway\""
  query1 <- "SELECT * FROM \"underway:nuyina_underway\" WHERE datetime > '{time1}'"

  if (!init) {
    dat <- arrow::read_parquet(filename)

    if (nrow(dat) < 1) {
      query <- query0
    } else {

      time1 <- format(max(dat$datetime, "%Y-%m-%dT%H:%M:%SZ"))
      query <- glue::glue(query1)
    }
  }
  #Sys.setenv("OGR_WFS_USE_STREAMING" = "YES")


#print(query)
  uwy <- vapour::vapour_read_fields("WFS:https://data.aad.gov.au/geoserver/ows?service=wfs&version=2.0.0&request=GetCapabilities",
                                    sql = query)

  uwy <- dplyr::bind_rows(dat, tibble::as_tibble(uwy))
  #name changed to datetime and doesn't need parsing (but maybe that's version-specific)
  if (!inherits(uwy$datetime, "POSIXct")) {
    uwy$datetime <- as.POSIXct(uwy$datetime, "%Y/%m/%d %H:%M:%S", tz = "UTC")
  }

#  bad <- abs(dat$longitude) < .1 & abs(dat$latitude) < .1  ## FIXME
  #dat$longitude <- abs(dat$longitude)  ## FIXME when geoserver feed is fixed
 # if (any(bad)) dat <- dat[!bad, ]
  dat <- dplyr::arrange(dplyr::distinct(uwy, .data$datetime, .data$longitude, .data$latitude, .keep_all = TRUE), .data$datetime)

  unlink(filename)
  arrow::write_parquet(dat, filename)

  dat
}
