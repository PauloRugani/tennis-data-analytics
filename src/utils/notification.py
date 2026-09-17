import requests

def send_telegram_message(message: str, bot_token: str, chat_id: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(e)
        raise

def on_success_callback(context):
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    formatted_date = execution_date.strftime("%Y-%m-%d %H:%M:%S")
    dag_run = context.get('dag_run')
    duration_seconds = (dag_run.end_date - dag_run.start_date).total_seconds()
    duration = f"{int(duration_seconds // 60)}m {int(duration_seconds % 60)}s"
    
    from airflow.models import Variable
    bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")

    message = f"""✅ *SUCCESSFULL PIPELINE RUN*\n
📌 *DAG*: `{dag_id}`
🕰️ *Execution Date*: `{formatted_date}`
🏃 *Duration*: `{duration}`\n
    """

    send_telegram_message(message, bot_token, chat_id)

def on_failure_callback(context):
    dag_id = context.get('dag').dag_id
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    formatted_date = execution_date.strftime("%Y-%m-%d %H:%M:%S")
    log_url = task_instance.log_url

    from airflow.models import Variable
    bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")

    message = f"""🚨 *PIPELINE FAILURE*\n
📌 *DAG*: `{dag_id}`    
❌ *Task*: `{task_id}`
🕰️ *Execution Date*: `{formatted_date}`\n
        """
    send_telegram_message(message, bot_token, chat_id)