# PROJECT: ff-check
# PURPOSE: Extraction of Patient Cohorts for ML
# AUTHOR: Baboyma Kagniniwa | USAID - SI
# LICENSE: MIT
# REF. ID: c12a5033
# CREATED: 2024-08-15
# UPDATED: 2024-08-15
# NOTES:

# Libraries ====

  library(tidyverse)
  library(readxl)
  library(gagglr)
  library(grabr)
  library(sf)
  library(scales)
  library(extrafont)
  library(tidytext)
  library(patchwork)
  library(ggtext)
  library(janitor)

# Set paths  ====

  dir_data   <- "Data"
  dir_dataout <- "Dataout"
  dir_images  <- "Images"
  dir_graphics  <- "Graphics"

  dir_mer <- glamr::si_path("path_msd")
  dir_ras <- glamr::si_path("path_raster")
  dir_shp <- glamr::si_path("path_vector")
  dir_datim <- glamr::si_path("path_datim")

  dir_cntries <- getOption("path_cntries")

# Parameter

  ref_id <- "c12a5033"
  cntry <- "Nigeria"
  agency <- "USAID"

  dir_nga <- dir_cntries %>%
    fs::path(cntry, dir_data)

  file_plist <- dir_nga %>%
    return_latest("ACE2_FY24Q3.*Patient List.*.xlsx")

# Functions  ====


# LOAD DATA ====

  file_plist %>% excel_sheets()

  df_plist <- file_plist %>%
    read_excel(sheet = 1)

  df_plist <- df_plist %>%
    select(-starts_with("...")) %>%
    clean_names() %>%
    rename_with(
      .cols = everything(),
      .fn = ~ str_remove(.x, "_yyyy_mm_dd$")
    )

  df_plist %>% glimpse()

  df_plist %>% summary()
  df_plist %>% skimr::skim()

  df_plist %>% distinct(archived)
  df_plist %>% distinct(current_status) %>% pull() %>% is_numeric()

# MUNGE ====

  df_study <- df_plist %>%
    filter(str_detect(current_status, "\\d", negate = T)) %>%
    select(patient_uuid = patient_id,
           date_of_birth, age, sex = gender,
           #pop_group = NA_character_,
           facility_uuid = datim_id,
           date_of_diagnosis = date_of_confirmed_hiv_test,
           date_tx_initiation = art_start_date,
           current_regimen,
           date_of_last_refill,
           last_refill_duration_days,
           current_status,
           #mmd_model = NA_character_,
           dsd_type,
           date_of_sample_collection,
           last_viral_load,
           viral_load_indication
           )

# VIZ ====



# EXPORT ====

  df_study %>%
    write_csv(x = .,
              na = "",
              file.path(dir_dataout, "Nigeria_FY24Q3_Patient_List_Report.csv"))
