These experiments are based on the work of <https://www.tandfonline.com/doi/full/10.1080/17538947.2014.962999>.

In this folder, to generate the air quality dataset, run 
```
python generate_dataset.py
```

You will need to source the virtual environment first:

```
source ../traffic_monitoring_example/config/virtualenv/venv/bin/activate
```

Then, to import the data into your data warehouse of choice, run

```
./load_data.sh
```

