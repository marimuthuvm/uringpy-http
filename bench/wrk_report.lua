-- Print wrk's (or wrk2's) summary as one JSON line, so the driver needs no
-- text parsing. Latencies are in microseconds; -1 marks a value the tool
-- could not compute (wrk2 can report NaN for the mean of a short run).
local function num(x)
  if x == nil or x ~= x or x == math.huge or x == -math.huge then return -1 end
  return x
end

done = function(summary, latency, requests)
  local e = summary.errors
  io.write(string.format(
    'WRKJSON {"requests":%d,"duration_us":%d,"bytes":%d,' ..
    '"err_connect":%d,"err_read":%d,"err_write":%d,"err_status":%d,"err_timeout":%d,' ..
    '"lat_mean_us":%.2f,"lat_p50_us":%d,"lat_p90_us":%d,"lat_p99_us":%d,"lat_p999_us":%d,"lat_max_us":%d}\n',
    summary.requests, summary.duration, summary.bytes,
    e.connect, e.read, e.write, e.status, e.timeout,
    num(latency.mean), num(latency:percentile(50)), num(latency:percentile(90)),
    num(latency:percentile(99)), num(latency:percentile(99.9)), num(latency.max)))
end
