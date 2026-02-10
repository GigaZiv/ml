from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):
    hook = TelegramHook(token='8397031351:AAEUIc61DDwkihL6AFS2tmPp9FTRVQlJoZA', chat_id='-5294360270')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    message = f'Исполнение {task_instance_key_str} с id={run_id} failure!'
    hook.send_message({
        'chat_id': '-5294360270',
        'text': message
    })

def send_telegram_success_message(context):
    hook = TelegramHook(token='8397031351:AAEUIc61DDwkihL6AFS2tmPp9FTRVQlJoZA', chat_id='-5294360270')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    message = f'Исполнение DAG {task_instance_key_str} с id={run_id} success!'
    hook.send_message({
        'chat_id': '-5294360270',
        'text': message
    })