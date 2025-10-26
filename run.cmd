call python test_hlc_merge.py --duration=1 --target-server-fps=90 --events-output-file=hlc_simulation_output.json --merged-events-output-file=hlc_simulation_merged.json
@rem --verbose 
call jq -r -f filter.jq <hlc_simulation_output.json  >hlc_simulation_output.csv
echo kind,node_id,event_counter,frame_counter,timestamp,delta_seconds,real_delta_seconds >hlc_simulation_output_original.csv
call jq -r -f convert_original.jq <hlc_simulation_output.json  >>hlc_simulation_output_original.csv

echo source_id,kind,hlc_clock_l,timestamp,source_id,sender,message_id,frame_counter >hlc_simulation_merged.csv
call jq -r -f convert_merged.jq <hlc_simulation_merged.json  >>hlc_simulation_merged.csv
