WITH date_table AS (

  {{ dbt_utils.date_spine(
      start_date="DATE(2020, 10, 01)",
      datepart="day",
      end_date="DATE(2022, 04, 08)"
     )
  }}

)

select * from date_table
