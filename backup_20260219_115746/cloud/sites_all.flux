from(bucket:"cci")
  |> range(start:-2h)
  |> keep(columns:["site_id"])
  |> distinct(column:"site_id")
