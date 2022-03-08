from datetime import datetime
import time
import yaml 
from multiprocessing import Process

def yaml_lader():
    with open('Milestone1B.yaml','r') as file_descriptor :
        data = yaml.safe_load(file_descriptor)
    return data 
  
  
def log_task(flow_name, task_name, msg):
  log(f'{flow_name}.{task_name} {msg}')
  
  
def print_world(msg='HelloWorld', label='DEFAULT'):
  time.sleep(2)
  log(f'[ {label} ]: {msg}')
  
def dispatch_concurrently(fns):
  proc = []
  print(fns)
  for fn in fns:
    func = fn.get('function')
    args = fn.get('args')
    p = Process(target=func, args=args)
    p.start()
    proc.append(p)
  for p in proc:
    p.join()

def dispatch_flow(flow_name, activities, execution='Sequential'):
  log(f'{flow_name} Entry')
  
  if execution == 'Concurrent':
    tasks_to_execute = []
    for task_name in activities.keys():
      task = activities.get(task_name)
      
      function_to_call = print
      args = []
      
      if task.get('Type') == 'Task': 
        function_to_call = dispatch_task
        args = [flow_name, task_name, task]
      elif task.get('Type') == 'Flow':
        function_to_call = dispatch_flow
        args = [f'{flow_name}.{task_name}', task.get('Activities'), task.get('Execution')]
      
      tasks_to_execute.append({
        'function': function_to_call,
        'args': args
      })
    
    dispatch_concurrently(tasks_to_execute)
  elif execution == 'Sequential':
    for task_name in activities.keys():
      task = activities.get(task_name)
      
      if task.get('Type') == 'Task':
        dispatch_task(flow_name, task_name, task)
      if task.get('Type') == 'Flow':
        dispatch_flow(f'{flow_name}.{task_name}', task.get('Activities'), task.get('Execution'))
  
  log(f'{flow_name} Exit')

def dispatch_task(flow_name, task_name, task):
  log_task(flow_name, task_name, 'Entry')
  function_name = task.get('Function')
  function_args = ", ".join(list(task.get('Inputs').values()))
  execution_time = task.get('Inputs').get('ExecutionTime')
  log_task(flow_name, task_name, f'Executing {function_name}({function_args})')
  time.sleep(float(execution_time))
  log_task(flow_name, task_name, 'Exit')


def log(msg):
  msg = f'{datetime.now()};{msg}\n'
  print(msg)
  with open('log1.txt', 'a') as f:
    f.write(msg)

def log_flow(flow_name, flow):
  flow_type = flow.get('Type')
  if flow_type is None:
    return

  if flow_type.lower() == 'flow':
    dispatch_flow(flow_name, flow.get('Activities'), flow.get('Execution'))

def main():
  pipeline = yaml_lader()
  
  for key in pipeline.keys():
    log_flow(key, pipeline[key])

if __name__ == '__main__':
  main()