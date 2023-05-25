select h3, validity, avg(value)
from main
where variable = 'TRAFFIC_INCIDENTS'
group by CUBE (h3, validity);