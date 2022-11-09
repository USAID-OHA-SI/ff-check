##
##  PROJECT: DATIM Data Import
##  AUTHOR:  Baboyma Kagniniwa | USAID
##  PURPOSE: USAID/Nigeria - Flat files pre-validations
##  LICENCE: MIT
##  DATE:    2022-10-31
##  UPDATED: 2022-11-04
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
  library(datimvalidation)
  library(datimutils)
  library(sqldf)
  library(janitor)
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
  dir_secrets <- "../../DATIM/.credentials"

  # COPY below DATIM credentials into and text editor, update username / password, and save it as "secret.json" file
  #{"dhis": { "baseurl": "https://dev-de.datim.org/", "username": "admin", "password": "district" } }

  # Verify secret file is within specified directory
  #list.files(dir_secrets, full.names = TRUE)

  # Get full path of secret file & initial login
  #file_secrets = file.path(dir_secrets, "secret.json")

  #loginToDATIM(config_path = file_secrets)

  loginToDATIM(username = glamr::datim_user(),
               password = glamr::datim_pwd(),
               base_url = "https://www.datim.org/")

# Inputs files ----

  # Processing Time Stamp
  ts <- format(Sys.time(), "%Y%m%d%H%M%S")
  tss <- format(Sys.time(), "%Y-%m-%d %H%M%S")

  # Input Files Directory
  dir_in <- "./Data/"
  dir_in <- paste0(dir_in, tss, "/")

  if(!dir.exists(dir_in)) dir.create(dir_in)

  # Output Files Directory
  dir_out <- "./Dataout/"
  dir_out <- paste0(dir_out, tss, "/")

  if(!dir.exists(dir_out)) dir.create(dir_out)

  # Flat fiels structures
  req_cols <- c("dataelement", "period", "orgunit",
                "categoryoptioncombo", "attributeoptioncombo", "value")

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
             pattern = "flat.*file.*.csv$",
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

  # Reporting Periods - Calendar year
  df_partners %>% distinct(period)

  # Partners
  df_partners %>% distinct(attributeoptioncombo)

  # Reference IMs Table

  # Mechs doing flat files based Datim Import

  ff_partners <- c("ACE") %>%
    paste(1:6) %>%
    append("RISE")

  df_ims <- tibble::tribble(
             ~uid,                                                                                                                 ~mechanism, ~mech_name,
    "dxmWiSFC4Ec",            "160521 - 72062022CA00004 - Accelerating Control of the HIV Epidemic in Nigeria - ACE 1: Adamawa, Borno & Yobe", "ACE 1",
    "ZlQLnKsU2hp",   "160522 - 72062022CA00005 - Accelerating Control of the HIV Epidemic in Nigeria - ACE 2, Bauchi, Kano and Jigawa States", "ACE 2",
    "bSG3l4iz5o0",       "160523 - 72062022CA00003 - Accelerating Control of the HIV Epidemic in Nigeria ACE 3, - Kebbi, Sokoto and Zamfara.", "ACE 3",
    "adMgu5xx9EN",          "160524 - 72062022CA00006 - Accelerating Control of the HIV Epidemic in Nigeria - ACE 4, Kwara and Niger States.", "ACE 4",
    "v1kPnv5KfhH",                "160525 - 72062022CA00002 - Accelerating Control of the HIV Epidemic in Nigeria - 6 Lagos, Edo and Bayelsa", "ACE 6",
    "mYAtSbuTcTX", "160527 - 72062022CA00007 - Accelerating Control of the HIV Epidemic in Nigeria - ACE 5, Akwa-Ibom and Cross-River States", "ACE 5",
    "MJjm3e0OKvy",                                        "81858 - 7200AA19CA00003 - Reaching Impact, Saturation and Epidemic Control (RISE)", "RISE"
    )

  df_mechview <- datim_mechview(query = list(ou = "Nigeria"))

  df_mechview <- df_mechview %>%
    filter(funding_agency == "USAID",
           str_detect(prime_partner, "^TBD", negate = T),
           enddate >= lubridate::ymd("2021-10-01")) %>%
    mech_acronyms()

  # Check validity of attributeoptioncombo

  partners <- df_partners %>%
    distinct(attributeoptioncombo) %>%
    pull()

  df_rep_status <- df_partners %>%
    distinct(attributeoptioncombo) %>%
    right_join(df_mechview,
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

  if(!all(partners %in% df_mechview$uid)) {
    usethis::ui_error(paste0("There are some invalid AttributeOptionCombo UIDs: ",
                            paste(setdiff(partners, df_ims$uid), collapse = ", ")))
  }

  if(length(rep_pending_partner) > 0) {
    usethis::ui_warn(paste0("These partners did not report: ",
                             paste(rep_pending_partner, collapse = ", ")))
  }

  # Split agency flat files into im specific
  df_partners %>%
    split_out_inputs(ims = df_ims, dir_out = dir_out)


# Process files ----

  # Process Parameters
  cntry <- "Nigeria"
  type <- "csv"
  idScheme <- "id"
  dataElementIdScheme <- "id"
  orgUnitIdScheme <- "id"
  expectedPeriod <- "2022Q3"

  # list IM Flat files
  im_files <- list.files(dir_out,
                         pattern = paste0(".*", tss, ".csv$"),
                         full.names = TRUE)

  # Get Data Sets UIDs
  ds <- getCurrentDataSets(datastream = "RESULTS")

  # Validate files
  im_files %>%
    walk(~validate_submission(.sbm_file = .x,
                              sbm_type = "csv",
                              exclude_errors = TRUE,
                              id_scheme = "id"))

# Convert flat files to msd outputs ----

  # Reference datasets

  # MER Current DataSets ----
  df_datasets <- datim_sqlviews(view_name = "Data sets", dataset = TRUE)

  df_datasets <- df_datasets %>%
    filter(str_detect(name, "^MER Results") & str_detect(name, ".*FY.*", negate = T))

  # MER Data Elements ----
  df_deview <- df_datasets %>%
    pull(uid) %>%
    map_dfr(possibly(.f = ~datim_deview(datasetuid = .x),
                     otherwise = NULL))

  df_deview <- df_deview %>%
    select(dataelementuid, dataelement = shortname,
           categoryoptioncombouid, categoryoptioncombo)

  # MER Category Option Combos ----
  df_cocview <- datim_sqlviews(view_name = "MER category option combos",
                               dataset = T)

  # MER Attribute Option Combos
  df_aocview <- df_mechview %>%
    select(mech_uid = uid, mech_code, mech_name, prime_partner)

  # MER Org Units ----

  # Org Levels
  #df_levels <- get_cntry_levels(cntry, glamr::datim_user(), glamr::datim_pwd())
  df_levels <- get_cntry_levels(cntry)

  # Country
  df_cntries <- datim_cntryview()

  # Org Hierarchy
  df_orgview <- df_cntries %>%
    filter(orgunit_name == cntry) %>%
    pull(orgunit_uid) %>%
    datim_orgview(cntry_uid = .)

  # Reshape OrgH.
  df_orgview <- df_orgview %>%
    reshape_orgview(df_levels)

  df_orgview %>% distinct(orgunit_level)

  # Update OrgH. - Add parent org in wide format
  df_orgview <- df_orgview %>%
    reshape_orgview(df_levels) %>%
    update_orghierarchy(df_levels)


  # Load Processed datasets ----

  file_imports <- list.files(path = dir_out,
                             pattern = paste0(basename(dir_out), " - import.csv$"),
                             full.names = TRUE)

  df_imports <- file_imports %>%
    first() %>%
    map_dfr(read_csv, col_type = "c")

  df_imports %>% glimpse()

  df_imports %>%
    convert_ff2msd(list(
      orgview = df_orgview,
      deview = df_deview,
      aocview = df_aocview
    ))

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



