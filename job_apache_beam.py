import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime


class Elemento:

    def __init__(self,timestamp,customer,page,product=''):
        self.timestamp = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
        self.customer = customer
        self.page= page
        self.product = product


class PegarElemento(beam.DoFn):
    def process(self,element,linha_anterior):

        res = json.loads(element)
        try:
            linha_atual = Elemento(res['timestamp'],res['customer'],res['page'],res['product'])
        except: 
            linha_atual = Elemento(res['timestamp'],res['customer'],res['page'])
        
        if(linha_anterior!=None):
            if(linha_atual.customer != linha_anterior.customer):
                if(linha_anterior.page !='checkout'):
                    print('entrou')
                    arquivo_output = open('./output/abandoned-carts.json','a')
                    arquivo_output.write('{"timestamp":"%s","customer":"%s","product":"%s"}\n'% (str(linha_anterior.timestamp),linha_anterior.customer,linha_anterior.product)) 
                else:
                    linha_anterior = linha_atual
            else:
                linha_anterior = linha_atual
        else:
            linha_anterior = linha_atual

        return [element,linha_anterior]

with beam.Pipeline(options=PipelineOptions()) as p:
    entrada = p| beam.io.ReadFromText('./input/page-views.json')
    linha_anterior=None
    entrada | beam.ParDo(PegarElemento(),linha_anterior)

     
    