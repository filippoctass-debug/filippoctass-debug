from(bucket:"cci")
  |> range(start:-30m)
  |> keep(columns: ["_measurement","site_id","edge_id"])
  |> distinct(column: "_measurement")
