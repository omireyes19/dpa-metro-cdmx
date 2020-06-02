for i in $(seq 2010 2019)
do
	for j in $(seq 1 12)
	do
	  PYTHONPATH='.' AWS_PROFILE=omar luigi --module cleaned_ingest_metadata cleaned_task_metadata --year $i --month $j --local-scheduler
	done
done
