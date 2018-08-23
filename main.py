import click

from word_segmentation import segmentation
from workers import entrance, start_rabbit_workers

@click.group()
def clis():
    pass
clis.add_command(segmentation)
clis.add_command(entrance)
clis.add_command(start_rabbit_workers)

if __name__ == "__main__":
    clis()
