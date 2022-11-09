
#' @title Add Accronyms
#'
mech_acronyms <- function(.data) {

  if(!all(c("mech_code", "mech_name") %in% names(.data))) {
    .data %>% mutate(mech_shortname = NA_character_)

  } else {

    .data %>%
      mutate(
        mech_shortname = case_when(
          mech_code == "160521" ~ "ACE 1",
          mech_code == "160522" ~ "ACE 2",
          mech_code == "160523" ~ "ACE 3",
          mech_code == "160524" ~ "ACE 4",
          mech_code == "160527" ~ "ACE 5",
          mech_code == "160525" ~ "ACE 6",
          mech_code == "81860" ~ "KP CARE 2",
          mech_code == "81861" ~ "KP CARE 1",
          mech_code == "18655" ~ "SHARP TO1",
          mech_code == "81856" ~ "SHARP TO2",
          mech_code == "81857" ~ "SHARP TO3",
          mech_code == "18656" ~ "ICHSSA 1",
          mech_code == "18657" ~ "ICHSSA 2",
          mech_code == "81862" ~ "ICHSSA 3",
          mech_code == "81863" ~ "ICHSSA 4",
          mech_code == "81858" ~ "RISE",
          mech_code == "100222" ~ "EpiC",
          mech_code == "14505" ~ "SIDHAS",
          str_detect(mech_name, "\\(.*\\)") ~ str_extract(mech_name, "(?<=\\().*(?=\\))"),
          str_count(mech_name, "\\S+") <= 3 ~ mech_name,
          str_count(mech_name, "\\S+") > 3 ~ paste0(mech_code, " - ", word(mech_name, end = 2), "..."),
          TRUE ~ mech_code
        )) %>%
      relocate(mech_shortname, .after = mech_name)
  }
}

#' @title Parse out IM specific files
#'
split_out_inputs <- function(.data, ims, dir_out) {

  tss <- basename(dir_out)

  .data %>%
    distinct(attributeoptioncombo) %>%
    pull(attributeoptioncombo) %>%
    walk(function(.uid){

      im <- df_ims %>%
        filter(uid == .uid)

      if (nrow(im) == 1) {

        im <- im %>%
          pull(mech_shortname) %>%
          str_replace("&", "and") %>%
          str_replace(":", "")

        df_im <- .data %>%
          filter(attributeoptioncombo == .uid)

        print(usethis::ui_info(paste0(im, ", row count = ", nrow(df_im))))

        write_csv(x = df_im,
                  file = file.path(dir_out, paste0(im, " - ", tss, ".csv")),
                  na = "")
      }
    })
}


#' @title Validate Submission
#'
validate_submission <- function(.sbm_file,
                                sbm_type = "csv",
                                exclude_errors = TRUE,
                                id_scheme = "id") {

  print(.sbm_file)

  d <- d2Parser(file = .sbm_file,
                type = sbm_type,
                invalidData = exclude_errors,
                idScheme = id_scheme,
                dataElementIdScheme = id_scheme,
                orgUnitIdScheme = id_scheme)

  d <- runValidation(d)

  delta <- nrow(d$data$import) - nrow(d$data$parsed)

  print("Differences btw parsed and import datasets: ")
  print(delta)

  print("Writing out messages ....")

  write_csv(
    x = d$info$messages,
    file = str_replace(d$info$filename, ".csv", " - messages.csv"),
    na = ""
  )

  print("Writing tests ...")

  if (!is.null(d$tests)) {
    for (t in names(d$t)) {

      print(paste0("Test results for: ", t))

      file_error <- d$info$filename %>%
        str_replace(".csv", paste0(" - TESTS - ", t, ".csv"))

      df_test <- d$tests[[t]]

      if (nrow(df_test) > 0) {
        write_csv(
          x = df_test,
          file = file_error,
          na = ""
        )
      }
    }
  }

  print("Writing out valid data ....")

  write_csv(
    x = d$data$import,
    file = str_replace(d$info$filename, ".csv", " - import.csv"),
    na = ""
  )
}

#' @title Pull Org Levels
#'
#'
get_cntry_levels <- function(cntry, username=NULL, password=NULL){

  .levels <- grabr::get_levels(username = username, password = password)

  .levels <- .levels %>%
    filter(countryname == cntry) %>%
    pivot_longer(cols = where(is.numeric),
                 names_to = "label",
                 values_to = "level") %>%
    mutate(level = as.character(level),
           label = case_when(
             label == "prioritization" ~ "psnu",
             TRUE ~ label)) %>%
    select(level, label) %>%
    arrange(desc(level))

  return(.levels)
}


#' @title Reshape OrgUnit SQLView
#'
reshape_orgview <- function(.data, levels) {

  # Add levels
  .data <- .data %>%
    filter(orgunit_level != min(orgunit_level)) %>%
    select(-ends_with("code"), -moh_id, -starts_with("regionorcountry")) %>%
    rename_with(.fn = ~str_replace(., "internal_id", "uid"),
                .cols = contains("internal")) %>%
    mutate(orgunit_parent_level = as.character(as.integer(orgunit_level) - 1)) %>%
    left_join(levels, by = c("orgunit_level" = "level")) %>%
    rename(orgunit_label = label) %>%
    left_join(levels, by = c("orgunit_parent_level" = "level")) %>%
    rename(orgunit_parent_label = label) %>%
    select(orgunit_parent_uid, orgunit_parent_name = orgunit_parent,
           orgunit_parent_level, orgunit_parent_label,
           orgunit_uid, orgunit_name, orgunit_level, orgunit_label)

  # Map Child to Parent Orgs
  .df_org <- .data %>%
    inner_join(.data,
               by = c("orgunit_parent_uid" = "orgunit_uid"),
               suffix = c("", "_top"))

  # Append non-matched Orgs - Optional
  .data %>%
    anti_join(.df_org) %>%
    bind_rows(.df_org, .) %>%
    select(matches(".*_parent_.*_top"),
           matches(".*_parent_.*"),
           matches("^orgunit_.*_top"),
           everything())
}


#' @title Clean up OrgUnit SQLView
#'
#' @param .df_orgview
#' @param levels
#'
update_orghierarchy <- function(.df_orgview, levels) {

    level_fac <- levels$level[df_levels$label=="facility"]
    level_com <- levels$level[df_levels$label=="community"]
    level_psnu <- levels$level[df_levels$label=="psnu"]
    level_cntry <- levels$level[df_levels$label=="country"]

    levels %>%
      select(level, label) %>%
      pmap_dfr(function(level, label){

        .df_org <- .df_orgview %>% filter(orgunit_level == level)

        usethis::ui_info(glue("{level} => {label}: {nrow(.df_org)}"))

        .df_org %>%
          mutate(
            facilityuid = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_uid,
              TRUE ~ "~"
            ),
            facility = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_name,
              TRUE ~ "Data reported above facility level"
            ),
            communityuid = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_parent_uid,
              orgunit_level == level_com & orgunit_level == level ~ orgunit_uid,
              TRUE ~ "~"
            ),
            community = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_parent_name,
              orgunit_level == level_com & orgunit_level == level ~ orgunit_name,
              TRUE ~ "Data reported above community level"
            ),
            psnuuid = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_parent_uid_top,
              orgunit_level == level_com & orgunit_level == level ~ orgunit_parent_uid,
              orgunit_level == level_psnu & orgunit_level == level ~ orgunit_uid,
              TRUE ~ "~"
            ),
            psnu = case_when(
              orgunit_level == level_fac & orgunit_level == level ~ orgunit_parent_name_top,
              orgunit_level == level_com & orgunit_level == level ~ orgunit_parent_name,
              orgunit_level == level_psnu & orgunit_level == level ~ orgunit_name,
              TRUE ~ "Data reported above psnu level"
            )
          )
      }) %>%
      select(-matches("_parent_|_top")) %>%
      rename(orgunit_type = orgunit_label)
}

#' @title Convert Flat File to MSD
#'
#'
convert_ff2msd <- function(.df_ff, refs) {

  .df_ff %>%
    clean_names() %>%
    rename(orgunit_uid = org_unit,
           attributeoptioncombouid = attribute_option_combo,
           dataelementuid = data_element,
           categoryoptioncombouid = category_option_combo) %>%
    left_join(refs$orgview, by = "orgunit_uid") %>%
    relocate(attributeoptioncombouid, .after = last_col()) %>%
    left_join(refs$aocview, by = c("attributeoptioncombouid" = "mech_uid")) %>%
    relocate(dataelementuid, categoryoptioncombouid, .after = last_col()) %>%
    left_join(refs$deview,
              by = c("dataelementuid", "categoryoptioncombouid")) %>%
    relocate(value, .after = last_col())
}
