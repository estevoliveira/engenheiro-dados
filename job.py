#import apache_beam as beam
#from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime

lista_final=[]

class Elemento:

    def __init__(self,timestamp,customer,page,product=''):
        self.timestamp = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
        self.customer = customer
        self.page= page
        self.product = product

lista_elemento = []

arquivo_input = open('./input/page-views.json','r')

for linha in arquivo_input:
    res = json.loads(linha)
    try:
        lista_elemento.append(Elemento(res['timestamp'],res['customer'],res['page'],res['product']))
    except: 
         lista_elemento.append(Elemento(res['timestamp'],res['customer'],res['page']))

for x in range(len(lista_elemento)):
    if(x>0):
        if(lista_elemento[x].customer != lista_elemento[x-1].customer):
            if(lista_elemento[x-1].page !='checkout'):
                arquivo_output = open('./output/abandoned-carts.json','a')
                arquivo_output.write('{"timestamp":"%s","customer":"%s","product":"%s"}\n'% (str(lista_elemento[x-1].timestamp),lista_elemento[x-1].customer,lista_elemento[x-1].product))





