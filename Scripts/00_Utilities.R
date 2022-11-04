
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
          pull(mech_name) %>%
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
