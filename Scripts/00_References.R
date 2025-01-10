# PROJECT: ff-check
# PURPOSE: DATIM IMPORT - Skipping the API
# AUTHOR: Baboyma Kagniniwa | USAID/GH - Office of HIV-AIDS
# LICENSE: MIT
# REF. ID: 7361bcc9
# CREATED: 2024-12-18
# UPDATED: 2024-12-19
# NOTES:

# Libraries ====

  library(tidyverse)
  library(readxl)
  library(glamr)
  library(gophr)
  library(glue)
  library(janitor)
  library(fs)
  library(duckdb)
  library(dbplyr)

# Set paths  ====

  dir_data   <- "Data"
  dir_dataout <- "Dataout"
  dir_images  <- "Images"
  dir_graphics  <- "Graphics"

  dir_refs <- dir_data %>% file.path("References")

  if (!dir_exists(dir_refs)) dir_create(dir_refs)

  dir_ls(dir_refs)

  dir_mer <- glamr::si_path("path_msd")
  dir_ras <- glamr::si_path("path_raster")
  dir_shp <- glamr::si_path("path_vector")
  dir_datim <- glamr::si_path("path_datim")

  dir_datim_refs <- "DATIM/Data-Import-and-Exchange-Resources"
  dir_datim_refs <-  dir_datim %>% file.path(dir_datim_refs)

  if (!dir_exists(dir_datim_refs)) usethis::ui_stop("MISSING DIRECTORRY - {dir_datim_refs}")

# Params

  ref_id <- "7361bcc9"
  ou <-  "Nigeria"
  cntry <- ou
  agency <- "USAID"

# FILES

  file_nat <- dir_mer %>%
    return_latest(glue("NAT_SUBNAT_FY22.*.zip$"))

  file_psnu <- dir_mer %>%
    return_latest(glue("PSNU_IM_FY22.*_{ou}.zip$"))

  file_site <- dir_mer %>%
    return_latest(glue("Site_IM_FY22.*_{ou}.zip$"))

  file_db <- dir_refs %>%
    file.path("datim.duckdb")

  dir_datim_refs %>% list.files()

# META

  meta <- file_psnu %>% get_metadata()

# QUERIES ====

  #' @title List of re-usable queries
  #'
  queries <- list(
    "levels_reshape" = "
      WITH UnestedLevels AS (
        SELECT
          fiscal_year, geolevel,
          name3 as operatingunit,
          CASE
            WHEN ((name4 IS NULL)) THEN name3
            WHEN NOT ((name4 IS NULL)) THEN name4
          END AS countryname,
          iso3 AS operatingunit_iso,
          CASE
            WHEN ((iso4 IS NULL)) THEN iso3
            WHEN NOT ((iso4 IS NULL)) THEN iso4
          END AS country_iso,
          country,
          CASE
            WHEN (prioritization = country) THEN (country + 1)
            WHEN ((prioritization - country) = 1) THEN prioritization
            WHEN ((prioritization - country) > 1) THEN (country + 1)
            WHEN ((prioritization - country) < 1) THEN (country + 1)
            ELSE NULL
          END AS snu1,
          prioritization, community, facility
        FROM levels
      )
      UNPIVOT UnestedLevels
      ON country, snu1, prioritization, community, facility
      INTO
        NAME label
        VALUE level
    ",
    "levels_pivot2" = "
      UNPIVOT levels
      ON country, prioritization, community, facility
      INTO
        NAME label
        VALUE level;
    ",
    "countries_clean" = "
      SELECT
        fiscal_year, geolevel,
        orgunit_code AS orgunitcode,
        orgunit_uid AS orgunituid,
        orgunit_name AS orgunit,
        orgunit_parent
      FROM
        countries
    ",
    "mechanism_clean" = "
      SELECT
        fiscal_year,
        geolevel,
        mechanism,
        mech_code,
        uid,
        prime_partner_name,
        prime_partner_uid,
        funding_agency,
        operatingunit,
        startdate,
        enddate,
        LTRIM(RTRIM(REGEXP_REPLACE(mech_name, '^-[ ]+-[ ]+-[ ]+-|^-[ ]+-[ ]+-[ ]+|^-[ ]+-[ ]+-|^-[ ]+-[ ]+|^-[ ]+-|^-[ ]+|^[ ]+-[ ]+-[ ]+-|^[ ]+-[ ]+-[ ]+|^[ ]+-[ ]+-|^[ ]+-[ ]+|^[ ]+-|^[ ]+', ''))) AS mech_name,
        award_number
      FROM (
        SELECT
          fiscal_year,
          geolevel,
          mechanism,
          mech_code,
          uid,
          prime_partner_name,
          prime_partner_uid,
          funding_agency,
          operatingunit,
          startdate,
          enddate,
          CASE
            WHEN (NOT((award_number IS NULL))) THEN (REGEXP_REPLACE(mech_name, award_number, ''))
            WHEN ((award_number IS NULL) AND REGEXP_MATCHES(mech_name, 'TBD')) THEN (REGEXP_REPLACE(mech_name, '- TBDaward.* - ', ''))
            ELSE mech_name
          END AS mech_name,
          award_number
        FROM (
          SELECT
            fiscal_year,
            geolevel,
            mechanism,
            mech_code,
            uid,
            prime_partner_name,
            prime_partner_uid,
            funding_agency,
            operatingunit,
            startdate,
            enddate,
            mech_name,
            LTRIM(RTRIM(REGEXP_REPLACE(award_number, '-', '', 'g'))) AS award_number
          FROM (
            SELECT
              fiscal_year,
              geolevel,
              mechanism,
              mech_code,
              uid,
              prime_partner_name,
              prime_partner_uid,
              funding_agency,
              operatingunit,
              startdate,
              enddate,
              REGEXP_REPLACE(mech_name, ' ', '', 'g') AS mech_name,
              CASE
                WHEN (REGEXP_MATCHES(prime_partner_name, '^TBD')) THEN NULL
                ELSE (LTRIM(RTRIM(REGEXP_EXTRACT(mech_name, '- ([A-Za-z0-9]+) -'))))
              END AS award_number
            FROM (
              SELECT q01.*, REGEXP_REPLACE(mechanism, mech_code, '') AS mech_name
              FROM (
                SELECT
                  fiscal_year,
                  geolevel,
                  mechanism,
                  code AS mech_code,
                  uid,
                  partner AS prime_partner_name,
                  primeid AS prime_partner_uid,
                  agency AS funding_agency,
                  ou AS operatingunit,
                  startdate,
                  enddate
                FROM mechanisms
              ) q01
            ) q01
          ) q01
        ) q01
      ) q01
    "
  )

# Functions  ====

  #' @title Initiate duckdb connection
  #'
  #' @param dbfile
  #' @param ...
  #'
  duck_connection <- function(dbfile = ":memory:", ...) {
    # Connection
    conn = NULL

    # (stringr::str_detect(dbfile, ".duckdb$|.db$") &
    #     !fs::file_exists(dbfile))
    # ))

    # Validation
    if(dbfile != ":memory:" &
       stringr::str_detect(dbfile, ".duckdb$|.db$", negate = TRUE)) {
      usethis::ui_stop(stringr::str_glue("DB [{dbfile}] is not valid or does not exists. Double check the input"))
    }

    # Establish connection
    conn = tryCatch(
      duckdb::dbConnect(
        drv = duckdb::duckdb(dbdir = dbfile),
        ...
      ),
      warning = function(wrn){stop(conditionMessage(wrn))},
      error = function(err){stop(conditionMessage(err))},
      finally = print("QUACK QUACK!!!")
    )

    return(conn)
  }


  #' @title shutdown duckdb connection
  #'
  #' @param dbfile
  #'
  duck_stop <- function(.conn) {
    tryCatch(
      DBI::dbDisconnect(conn = .conn, shutdown = TRUE),
      warning = function(wrn){stop(conditionMessage(wrn))},
      error = function(err){stop(conditionMessage(err))},
      finally = print("QUACK QUACK!!!")
    )
  }


  #' @title List Duck DB Tables
  #'
  #' @param conn
  #'
  duck_tables <- function(.conn) {
    DBI::dbGetQuery(
      conn = conn,
      statement = "SHOW ALL TABLES;"
    ) %>%
    as_tibble() %>%
    pull(name)
  }


  #' @title Map data sources
  #'
  #' @param .src directory of source files
  #'
  map_sources <- function(.src) {
    .src_files <- .src %>%
      list.files(pattern = ".csv$", full.names = TRUE) %>%
      map(function(.file){
        list(
          "source" = .file,
          "fiscal_year" = .file %>% basename() %>% str_sub(start = 1, end = 4),
          "geolevel" = case_when(
            str_detect(str_to_lower(basename(.file)), "data exchange organisation unit") ~ str_extract(.file, "(?<=\\FY\\d{2} - ).*(?= - )"),
            TRUE ~ "Global"
          ),
          "tablename" = case_when(
            str_detect(str_to_lower(basename(.file)), "levels") ~ "levels",
            str_detect(str_to_lower(basename(.file)), "mechanisms partners agencies") ~ "mechanisms",
            str_detect(str_to_lower(basename(.file)), "organisation units") ~ "orgunits",
            str_detect(str_to_lower(basename(.file)), "organisation unit attributes") ~ "attributes",
            str_detect(str_to_lower(basename(.file)), "ou countries") ~ "countries",
            str_detect(str_to_lower(basename(.file)), "results") ~ "datasets",
            TRUE ~ "unknown"
          ),
          "dataset" = case_when(
            str_detect(.file, "Results") ~ str_extract(.file, "(?<=\\FY\\d{2} - Results - ).*(?= - )"),
            TRUE ~ NA_character_
          )
        )
      })

    .src_files
  }


  #' @title Load data into DuckDB Table
  #'
  load_data <- function(.conn, .name, .data,
                        overwrite = TRUE,
                        append = FALSE) {
    DBI::dbWriteTable(
      conn = .conn,
      name = .name,
      value = .data,
      overwrite = overwrite,
      append = append
    )
  }


  #' @title Load reference source data into DuckDB
  #'
  load_ref_data <- function(.conn, .sources, reset = TRUE){

    # Reset all tables in the database
    if (reset == TRUE) {
      .tbls <- duck_tables(.conn)

      #DBI::dbBegin(conn = .conn)

      walk(.tbls, function(.tbl) {
        DBI::dbRemoveTable(
          conn = .conn,
          name = .tbl
        )
      })

      #DBI::dbCommit(conn = .conn)
    }

    # Read data from source and load into DB
    .sources %>%
      walk(function(.src){
        print(.src$source)
        #print(.src$tablename)

        .data <- .src$source %>%
          read_csv(guess_max = Inf, show_col_types = FALSE) %>%
          mutate(
            fiscal_year = .src$fiscal_year,
            geolevel = .src$geolevel
          ) %>%
          relocate(fiscal_year, geolevel, .before = 1)

        ## Bind datasets together into 1 table
        if (.src$tablename == "datasets") {
          .data <- .data %>%
            mutate(dataset = .src$dataset) %>%
            relocate(dataset, .after = geolevel)

          DBI::dbWriteTable(
            conn = .conn,
            name = .src$tablename,
            value = .data,
            overwrite = FALSE,
            append = TRUE
          )
        }
        ## Keep other data as individual tables
        else {
          DBI::dbWriteTable(
            conn = .conn,
            name = .src$tablename,
            value = .data,
            overwrite = FALSE,
            append = FALSE
          )
        }


      })
  }


  #' @title Get Queries
  #'
  duck_orgunits <- function(.endpoint, .conn, .query) {
    #ou/cntry/level/
  }

  #' @title Get OU Tables
  #'
  duck_outable <- function(.conn, .query) {
    #ou/cntry/level/
  }
                                                                                                                                                  # DB Connection
# CONNECTION ----

  ## The connection will create a new file or connect to existing one
  conn <- duck_connection(dbfile = file_db)

  conn %>% DBI::dbGetInfo()

  conn %>% duck_tables()

  ddb_sets_query <- conn %>%
    dbSendQuery("SELECT * FROM duckdb_settings()")

  ddb_sets <- ddb_sets_query %>%
    dbFetch() %>%
    as_tibble()

  ddb_sets_query %>% dbClearResult()

  ddb_sets %>%
    filter(str_detect(name, "memory|schema"))

  ram_max <- ddb_sets %>%
    filter(name == "max_memory") %>%
    pull(value) %>%
    str_remove(" GiB$") %>%
    as.integer()

  ram_limit <- ram_max %>%
    magrittr::divide_by(3) %>%
    round()

  conn %>% dbExecute(glue_sql("SET memory_limit = '{ram_limit}GB'", .con = conn))

# LOAD DATA ====

  dir_datim_refs %>% dir_ls()

  ## Load source ref. data ----

  ### 1 - Get Metadata
  src_files <- dir_datim_refs %>% map_sources()

  src_meta <- src_files %>%
    map(function(.src){
      .src %>% as_tibble()
    }) %>%
    bind_rows()

  ### 2 - Read and load source data
  src_files %>%
    load_ref_data(.conn = conn, .sources = ., reset = T)

  ### 3 - Validate
  duck_tables(.conn = conn)

  conn %>%
    duck_tables() %>%
    str_detect("unknown") %>%
    any() %>%
    isTRUE()

  ## Load Current FY PSNUxIM Results & Targets ----
  file_psnu %>%
    read_psd() %>%
    filter(fiscal_year == meta$curr_fy, country == cntry) %>%
    load_data(.conn = conn, .name = "psnuxim", .data = ., overwrite = TRUE)

  ## Load Current FY SITExIM Results & Targets ----
  file_site %>%
    read_psd() %>%
    filter(fiscal_year == meta$curr_fy, country == cntry) %>%
    load_data(.conn = conn, .name = "sitexim", .data = ., overwrite = TRUE)

  ## Load Current FY NAT SUBNAT Targets ----
  file_nat %>%
    read_psd() %>%
    filter(country == cntry) %>%
    load_data(.conn = conn, .name = "natsubnat", .data = ., overwrite = TRUE)

# MUNGE ====

  ## Transform datasets in DuckDB to avoid reading the entire CSV into R's memory
  src_files %>%
    map(as_tibble) %>%
    bind_rows() %>%
    filter(tablename == "levels") %>%
    pull(source) %>%
    duckdb::tbl_file(conn, .) %>%
    dplyr::mutate(
      name4 = ifelse(is.na(name4), name3, name4),
      iso4 = ifelse(is.na(iso4), iso3, iso4)
    ) %>%
    dplyr::rename(
      operatingunit = name3,
      countryname = name4,
      operatingunit_iso = iso3,
      country_iso = iso4
    ) %>%
    filter(countryname == cntry) %>%
    collect()

  ## Transform data in DuckDB and save result as a view
  tbl(conn, "levels") %>% #collect()
    dplyr::mutate(
      name4 = ifelse(is.na(name4), name3, name4),
      iso4 = ifelse(is.na(iso4), iso3, iso4)
    ) %>%
    dplyr::rename(
      operatingunit = name3,
      countryname = name4,
      operatingunit_iso = iso3,
      country_iso = iso4
    ) %>%
    dplyr::mutate(
      snu1 = dplyr::case_when(
        prioritization == country ~ country + 1,
        prioritization - country == 1 ~ prioritization,
        prioritization - country > 1 ~ country + 1,
        prioritization - country < 1 ~ country + 1, TRUE ~ NA_integer_
      )
    ) %>%
    tidyr::pivot_longer(
      #cols = dplyr::where(is.numeric),
      cols = c(country, prioritization, community, facility, snu1),
      names_to = "label",
      values_to = "level"
    ) %>% explain()
    dplyr::filter(!is.na(level)) %>%
    collect()

  ## Transform data in DuckDB with SQL
  duckdb::tbl_query(conn, glue_sql(queries$levels_pivot)) %>% glimpse()

  ## Orgunits
  tbl(conn, "orgunits") %>% glimpse()

  ## Countries
  tbl(conn, "countries") %>% glimpse()

  ## Mechanisms
  tbl(conn, "mechanisms") %>% glimpse()

  # Reshape Results - mech code, award number, and name separations chars
  sep_chrs <- c("[[:space:]]+",
                "[[:space:]]+-",
                "[[:space:]]+-[[:space:]]+",
                "[[:space:]]+-[[:space:]]+-",
                "[[:space:]]+-[[:space:]]+-[[:space:]]+",
                "[[:space:]]+-[[:space:]]+-[[:space:]]+-",
                "-[[:space:]]+",
                "-[[:space:]]+-",
                "-[[:space:]]+-[[:space:]]+",
                "-[[:space:]]+-[[:space:]]+-",
                "-[[:space:]]+-[[:space:]]+-[[:space:]]+",
                "-[[:space:]]+-[[:space:]]+-[[:space:]]+-")

  sep_chrs <- rev(sep_chrs)

  sep_chrs2 <- sep_chrs %>%
    str_replace_all("\\[:space:\\]", " ") %>%
    paste0("^", ., collapse = "|") %>%
    stringr::str_flatten()

  tbl(conn, "mechanisms") %>%
    dplyr::rename(
      mech_code = code,
      operatingunit = ou,
      prime_partner_name = partner,
      prime_partner_uid = primeid,
      funding_agency = agency,
      operatingunit = ou
    ) %>%
    dplyr::mutate(
      mech_name = stringr::str_remove(mechanism, mech_code),
      mech_name = stringr::str_replace_all(mech_name, "\n", ""),
      award_number = dplyr::case_when(
        stringr::str_detect(prime_partner_name, "^TBD") ~ NA_character_,
        TRUE ~ str_trim(dplyr::sql("REGEXP_EXTRACT(mech_name, '- ([A-Za-z0-9]+) -')"))
      ),
      award_number = stringr::str_trim(stringr::str_remove_all(award_number, "-")),
      mech_name = dplyr::case_when(
        !is.na(award_number) ~ stringr::str_remove(mech_name, award_number),
        is.na(award_number) & str_detect(mech_name, "TBD") ~ stringr::str_remove(mech_name, "- TBDaward.* - "),
        TRUE ~ mech_name
      ),
      mech_name = stringr::str_trim(stringr::str_remove(mech_name, sep_chrs2))
    ) %>%
    dplyr::select(uid, mech_code, mech_name,
                  award_number, mechanism,
                  dplyr::everything()) %>%
    arrange(desc(funding_agency), operatingunit, desc(startdate), mech_name) %>%
    collect()

# CLOSE ====

duck_stop(conn)
