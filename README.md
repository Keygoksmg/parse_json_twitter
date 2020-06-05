# twitter-covid
This project aim to convert big json data from twitter into csv file.

## Environment
python3

### docker
```
docker run --name covid -it -p 9990:8888 -v $(pwd):/home/jovyan/work kusukun7/acknow-lab jupyter lab --allow-root --ip=0.0.0.0 --no-browser
```