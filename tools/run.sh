## create table
echo 'create databases...'
python table_factory.py

echo 'store state, county, city data ...'

python store_data.py store_state 

echo 'store youreconomy ...'

python store_data.py store_ye

echo 'store indeed ...' 

python store_data.py store_indeed
