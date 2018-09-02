import click
import logging

#from word_segment import segmentation
from workers import entrance, bulk_calculate_tf, start_rabbit_workers

logging.basicConfig(level=logging.INFO)

@click.group()
def clis():
    pass


#clis.add_command(segmentation)
clis.add_command(entrance)
clis.add_command(bulk_calculate_tf)
clis.add_command(start_rabbit_workers)

if __name__ == "__main__":
    clis()
