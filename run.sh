## create table
echo 'create databases...'
python tools/table_factory.py

echo 'store state, county, city data ...'

python tools/store_data.py store_state 

echo 'store youreconomy ...'

python tools/store_data.py store_ye

echo 'store indeed ...' 

python tools/store_data.py store_indeed
