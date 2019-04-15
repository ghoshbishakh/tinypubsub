export FLASK_APP=broker/broker.py
# export FLASK_ENV=development
if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
	python -m flask run -p 9999 --host 0.0.0.0
else
	python -m flask run -p $1 --host 0.0.0.0
fi
