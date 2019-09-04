import csv
import os
import asyncio


class DataPack:
    def __init__(self, path):
        self.path = path
        # Constants
        self.OUTPUT = 'output.csv'

    def save_data(self, data):
        with open(self.path + '/' + self.OUTPUT, 'w') as f:
            f.writelines([x + '\n' for x in data])

    async def csv_to_list(self, file_name):
        with open(self.path + '/' + file_name, 'r') as f:
            reader = csv.DictReader(f)
            return[line["btcusdt"] for line in reader]

    async def fetch_data(self, name):
        print(f'Start process file {name}')
        return await self.csv_to_list(name)

    async def asynchronous(self, files):
        result_array = []

        # one coroutine for each file
        tasks = [self.fetch_data(f) for f in files]
        for pid, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            print(f'Process {pid} done')
            result_array += result

        # print(len(result_array), result_array)
        self.save_data(result_array)
        print('Complete')

    def run(self):
        files_in_dir = [x for x in os.listdir(self.path) if x.split('.')[-1] == 'csv']

        ev_loop = asyncio.get_event_loop()
        ev_loop.run_until_complete(self.asynchronous(files_in_dir))
        ev_loop.close()


# Usage
# my_path = '/home/kusko/PyProjects/untitled1/csvdata/'

datapack = DataPack(input('Dir path:\n'))
datapack.run()

