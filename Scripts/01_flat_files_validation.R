##
##  PROJECT: DATIM Data Import
##  AUTHOR:  Baboyma Kagniniwa | USAID
##  PURPOSE: USAID/Nigeria - Flat files pre-validations
##  LICENCE: MIT
##  DATE:    2022-10-31
##  UPDATED: 2023-11-01
##

# Installation ----

  # Note: Install these if you DO NOT have them installed already

  # install.packages("devtools")
  #
  # devtools::install_github("pepfar-datim/datim-validation", force=TRUE)
  # devtools::install_github("pepfar-datim/datimutils", force=TRUE)
  #
  # install.packages("sqldf")
  # install.packages("tidyverse")

# Libraries ----

  library(tidyverse)
  library(grabr)
  library(glamr)
  library(datimvalidation)
  library(datimutils)
  library(sqldf)
  library(janitor)
  library(lubridate)
  library(glue)
  # library(readr)
  # library(dplyr)
  # library(purrr)
  # library(stringr)
  # library(magrittr)

  source("../MerQL/Scripts/00_Utilities.R")
  source("./Scripts/00_Utilities.R")

# Credentials ----

  # Define directory for Datim Credentials files
  #dir_secrets <- "../../DATIM/.credentials"

  # COPY below DATIM credentials into and text editor, update username / password, and save it as "secret.json" file
  #{"dhis": { "baseurl": "https://dev-de.datim.org/", "username": "admin", "password": "district" } }

  # Verify secret file is within specified directory
  #list.files(dir_secrets, full.names = TRUE)

  # Get full path of secret file & initial login
  #file_secrets = file.path(dir_secrets, "secret.json")

  #loginToDATIM(config_path = file_secrets)

  url <- "https://www.datim.org"

  loginToDATIM(
    username = glamr::datim_user(),
    password = glamr::datim_pwd(),
    #base_url = "https://final.datim.org/" # Not always in sync
    base_url = paste0(url, "/")
  )

# Options

  # Agency / OU
  agency <- "USAID"
  cntry <- "Nigeria"

  # Process Parameters
  type <- "csv"
  idScheme <- "id"
  dataElementIdScheme <- "id"
  orgUnitIdScheme <- "id"
  expectedPeriod <- "2023Q3"

# Inputs files ----

  # Processing Time Stamp
  t <- Sys.time()
  today <- t %>% format("%Y-%m-%d")
  ts <- t %>% format("%Y%m%d%H%M%S")
  tss <- t %>% format("%Y-%m-%d %H%M%S")

  # Input Files Directory
  dir_in <- "./Data/"
  dir_in <- paste0(dir_in, tss, "/")

  if(!dir.exists(dir_in)) dir.create(dir_in)

  # Output Files Directory
  dir_out <- "./Dataout/"
  dir_out <- paste0(dir_out, tss, "/")

  if(!dir.exists(dir_out)) dir.create(dir_out)

  # Partners Submissions
  # NOTE: Downloads and note partners names

  ff_subms <- c("FY23Q4_Flat_Files_Collated")

  # Flat fiels structures
  req_cols <- c("dataelement",
                "period",
                "orgunit",
                "categoryoptioncombo",
                "attributeoptioncombo",
                "value")

  # Download and move files to input directory

  dir_down <- Sys.getenv("USERNAME") %>%
    paste0("C:/users/", ., "/Downloads")

  # dir_down <- Sys.getenv("USERPROFILE") %>%
  #   file.path("Downloads")
  #
  # if (extract2(Sys.getenv(), "sysname") != "Windows")
  #   dir_down <- "~/Downloads"

  # Copy files to input directory

  list.files(path = dir_down,
             pattern = paste0(ff_subms, collapse = "|"),
             full.names = TRUE)

  list.files(path = dir_down,
             pattern = paste0(ff_subms, collapse = "|"),
             full.names = TRUE) %>%
    walk(~fs::file_move(path = .x,
                        new_path = file.path(dir_in, basename(.x))))

  # list files to be processed
  flat_files <- list.files(path = dir_in,
                           pattern = ".*.csv$",
                           full.names = TRUE)

  # Split files by IM

  df_partners <- flat_files %>%
    map_dfr(function(.f){
      print(.f)

      df_ff <- read_csv(file = .f, col_types = "c") %>%
        rename_with(str_to_lower)

      if(!all(names(df_ff) %in% req_cols)) {
        usethis::ui_warn(paste0("Data structure from: ", .f,
                                " did not match required columns: ",
                                paste(req_cols, collapse = ", ")))
      }

      return(df_ff)
    })



  # Confirm Structure
  df_partners %>% names()
  df_partners %>% glimpse()

  df_partners <- df_partners %>%
    rename(attributeoptioncombo = attroptioncombo)

  # Reporting Periods - Calendar year
  df_partners %>% distinct(period)

  # Partners
  df_partners %>% distinct(attributeoptioncombo)

  # Reference IMs Table

  # Mechs doing flat files based Datim Import

  df_mechs <- datim_mechview(
    username = datim_user(),
    password = datim_pwd(),
    query = list(
      type = "variable",
      params = list(ou = "Nigeria")
    )
  )

  df_mechs <- df_mechs %>%
    filter(funding_agency == agency,
           operatingunit == cntry,
           !is.na(prime_partner),
           str_detect(prime_partner, "Dedupe|TBD", negate = TRUE),
           ymd(enddate) > ymd(today)) %>%
    select(uid, mech_code, mech_name, prime_partner) %>%
    mech_acronyms()

  ff_partners <- c("ACE") %>%
    paste(1:6) %>%
    append("RISE") %>%
    append(paste0("KP CARE ", 1:2))

  ff_partners

  df_mechs$mech_shortname

  df_mechs <- df_mechs %>%
    filter(mech_shortname %in% ff_partners)

  if (!any(pull(df_partners %>% distinct(attributeoptioncombo)) %in% df_mechs$uid)) {
    usethis::ui_warn("Unknown partner uid was detected")
  }

  # Check validity of attributeoptioncombo

  partners <- df_partners %>%
    distinct(attributeoptioncombo) %>%
    pull()

  df_rep_status <- df_partners %>%
    distinct(attributeoptioncombo) %>%
    right_join(df_mechs,
               by = c("attributeoptioncombo" = "uid"),
               keep = TRUE) %>%
    mutate(status = case_when(
      !is.na(attributeoptioncombo) & mech_shortname %in% ff_partners ~ "reported",
      !is.na(attributeoptioncombo) & !mech_shortname %in% ff_partners ~ "should not report",
      is.na(attributeoptioncombo) & mech_shortname %in% ff_partners ~ "did not report",
      TRUE ~ "other"
    )) %>%
    filter(status != "other") %>%
    select(attributeoptioncombo, mech_uid = uid, mech_name, mech_shortname, status)

  rep_pending_partner <- df_rep_status %>%
    filter(status == "did not report") %>%
    pull(mech_shortname)

  # Flag Invalid Partners
  if(!all(partners %in% df_mechs$uid)) {
    usethis::ui_error(paste0("There are some invalid AttributeOptionCombo UIDs: ",
                            paste(setdiff(partners, df_ims$uid), collapse = ", ")))
  }

  # Flag Pending Partners
  if(length(rep_pending_partner) > 0) {
    usethis::ui_warn(paste0("These partners did not report: ",
                             paste(rep_pending_partner, collapse = ", ")))
  }

  # Split agency flat files into im specific
  df_partners %>%
    split_out_inputs(.data = .,
                     ims = df_mechs,
                     dir_out = dir_out)


# Process files ----

  # list IM Flat files
  im_files <- list.files(dir_out,
                         pattern = paste0(".*", tss, ".csv$"),
                         full.names = TRUE)

  # Get Data Sets UIDs

  #ds <- getCurrentDataSets(datastream = "RESULTS")
  # ds <- datim_sqlviews(username = datim_user(),
  #                      password = datim_pwd(),
  #                      view_name = "Data sets",
  #                      dataset = T)

  # Validate files
  im_files %>%
    walk(~validate_submission(.sbm_file = .x,
                              sbm_type = "csv",
                              exclude_errors = TRUE,
                              id_scheme = "id"))

  ## Summarise messages
  list.files(dir_out,
             pattern = paste0(".*", tss, " - messages.csv$"),
             full.names = TRUE) %>%
    map_dfr(function(.x){
      read_csv(.x) %>%
        mutate(im = str_extract(basename(.x), paste0(".*(?= - ", tss, ")"))) %>%
        relocate(im, .before = 1)
    }) %>%
    write_csv(
      file = paste0(dir_out, "USAID Partners - ", tss, " - message summary.csv"),
      na = ""
    )

  ## Summarise validations rules
  list.files(dir_out,
             pattern = paste0(".*", tss, " - TESTS - validation_rules.csv$"),
             full.names = TRUE) %>%
    map_dfr(function(.x){
      read_csv(.x, col_types = "c") %>%
        mutate(im = str_extract(basename(.x), paste0(".*(?= - ", tss, ")"))) %>%
        mutate(across(everything(), as.character)) %>%
        relocate(im, .before = 1)
    }) %>%
    write_csv(
      file = paste0(dir_out, "USAID Partners - ", tss, " - TESTS - validation_rules summary.csv"),
      na = ""
    )

# Convert flat files to msd outputs ----

  # Reference datasets

  # MER Current DataSets ----
  df_datasets <- datim_sqlviews(username = datim_user(),
                                password = datim_pwd(),
                                view_name = "Data sets",
                                dataset = TRUE,
                                base_url = url)

  df_datasets %>%
    distinct(name) %>%
    pull()

  df_datasets <- df_datasets %>%
    filter(str_detect(name, "^MER Results") & str_detect(name, ".*FY.*", negate = T))

  # MER Data Elements ----
  df_deview <- df_datasets %>%
    pull(uid) %>%
    map_dfr(possibly(.f = ~datim_deview(username = datim_user(),
                                        password = datim_pwd(),
                                        datasetuid = .x,
                                        base_url = url),
                     otherwise = NULL))

  #df_deview %>% write_csv(file = "./Dataout/DATIM - MER Results Data Elements.csv")

  df_deview <- df_deview %>%
    select(dataelementuid, dataelement = shortname,
           categoryoptioncombouid, categoryoptioncombo)

  # MER Category Option Combos ----
  df_cocview <- datim_sqlviews(username = datim_user(),
                               password = datim_pwd(),
                               view_name = "MER category option combos",
                               dataset = T,
                               base_url = url)

  #df_cocview %>% write_csv(file = "./Dataout/DATIM - MER category option combos.csv")

  # MER Attribute Option Combos
  df_aocview <- df_mechs %>%
    select(mech_uid = uid, mech_code, mech_name, prime_partner)

  # MER Org Units ----

  # Org Levels
  #df_levels <- get_cntry_levels(cntry)
  df_levels <- get_cntry_levels(cntry = cntry,
                                glamr::datim_user(),
                                glamr::datim_pwd(),
                                base_url = paste0(url, "/"))

  # Country
  df_cntries <- datim_cntryview(username = datim_user(),
                                password = datim_pwd(),
                                base_url = url)

  # Org Hierarchy
  df_orgview <- df_cntries %>%
    filter(orgunit_name == cntry) %>%
    pull(orgunit_uid) %>%
    datim_orgview(username = datim_user(),
                  password = datim_pwd(),
                  cntry_uid = .)

  # Update OrgH. - Add parent org in wide format

  df_orgview <- df_orgview %>%
    reshape_orgview(df_levels) %>%
    update_orghierarchy(df_levels)

  # Load Processed datasets ----

  file_imports <- list.files(path = dir_out,
                             pattern = paste0(basename(dir_out), " - import.csv$"),
                             full.names = TRUE)

  df_imports <- file_imports %>%
    map_dfr(function(.file) {
      read_csv(.file, col_type = "c") %>%
        mutate(filename = basename(.file))
    })

  # df_imports %>% glimpse()
  # df_imports %>% distinct(filename)

  df_inputs <- list.files(path = dir_in,
               pattern = ".csv$",
               full.names = TRUE) %>%
    map_dfr(function(.file) {
      read_csv(.file, col_type = "c") %>%
        rename_with(~tolower(.)) %>%
        mutate(filename = basename(.file))
    })


  # Augment individual submissions

  file_imports %>%
    walk(function(.file) {

      print(.file)

      .df_import <- .file %>%
        read_csv(col_type = "c") %>%
        convert_ff2msd(list(
          orgview = df_orgview,
          deview = df_deview,
          aocview = df_aocview
        ))

      write_csv(x = .df_import,
                file = str_replace(.file, "import", "MSD Output"),
                na = "")
    })

  # Augment aggregated submissions

  df_imports %>%
    convert_ff2msd(list(
      orgview = df_orgview,
      deview = df_deview,
      aocview = df_aocview
    )) %>%
    write_csv(x = .,
              file = file.path(
                dir_out,
                paste0(cntry, " - ", expectedPeriod, " - MSD Output.csv")
              ),
              na = "")

