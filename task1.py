import requests
import psycopg2
import time
import logging
from psycopg2.extras import execute_values
from decouple import config
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bitcoin_parser.log",
    filemode="a"
)

DB_SETTINGS = {
    "dbname": config("DB_NAME"),
    "user": config("DB_USER"),
    "password": config("DB_PASSWORD"),
    "host": config("DB_HOST"),
    "port": config("DB_PORT", cast=int)
}
API_KEYS = [
    "BQY9Gi0gIcn78RS7uVbGT8VntPfd5QHM",
    "BQYxrd0Zp0s3v9KzwNLtrtzi2bQJ9FM6",
    "BQYejiIwfR4ZXvOEe0S1KAjK7vw4cKqK",
    "BQYGfnW4WI0ubRl805XKNb9JrQZ1tNUI"
]
current_api_key_index = 0
REQUEST_COUNT = 0
NETWORK = "bitcoin"
LIMIT = 100
OFFSET = 0
FROM_TIME = "2025-01-02T00:00:00Z"
TILL_TIME = "2025-01-10T23:59:59Z"
URL =  "https://graphql.bitquery.io"

class APIConfig:
    def __init__(self, api_keys: List[str], network: str, url: str):
        self.api_keys = api_keys
        self.current_index = 0
        self.network = network
        self.url = url
        self.headers = {"Content-Type": "application/json", "X-API-KEY": self.api_keys[self.current_index]}

    def switch_to_next_key(self) -> bool:
        """
        Переключается на следующий API-ключ. Возвращает False, если ключей больше нет.
        """
        if self.current_index < len(self.api_keys) - 1:
            self.current_index += 1
            self.headers["X-API-KEY"] = self.api_keys[self.current_index]
            print(f"Переключение на новый API-ключ: {self.api_keys[self.current_index]}")
            return True
        else:
            print("Все API-ключи исчерпаны!")
            return False

api_config = APIConfig(
    api_keys=API_KEYS,
    network=NETWORK,
    url=URL
)


def get_current_api_key() -> str:
    return API_KEYS[current_api_key_index]

def insert_data_to_db(conn, data: List[Dict[str, Any]]) -> None:
    cursor = conn.cursor()
    try:
        query = """
        INSERT INTO bitcoin_transactions (hash, address, category, value, timestamp)
        VALUES %s
        ON CONFLICT (hash, category, address, value, timestamp) DO NOTHING
        """
        values = [
            (item['hash'], item['address'], item['category'], item['value'], item['timestamp'])
            for item in data
        ]
        execute_values(cursor, query, values)
        conn.commit()
        logging.info(f"{len(values)} записей добавлено в таблицу `bitcoin_transactions` (с учётом уникальности).")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных в таблицу `bitcoin_transactions`: {e}")
    finally:
        cursor.close()


def update_balances_incremental(conn, input_totals: Dict[str, int], output_totals: Dict[str, int]) -> None:
    cursor = conn.cursor()
    try:
        addresses = set(input_totals.keys()).union(output_totals.keys())
        logging.info(f"Обновление балансов для {len(addresses)} адресов.")

        for address in addresses:
            total_input = input_totals.get(address, 0)
            total_output = output_totals.get(address, 0)
            query = """
            INSERT INTO bitcoin_balances (address, total_input, total_output, balance)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE
            SET total_input = bitcoin_balances.total_input + EXCLUDED.total_input,
                total_output = bitcoin_balances.total_output + EXCLUDED.total_output,
                balance = bitcoin_balances.balance + EXCLUDED.total_input - EXCLUDED.total_output;
            """
            cursor.execute(query, (address, total_input, total_output, total_input - total_output))

        conn.commit()
        logging.info(f"Балансы успешно обновлены для {len(addresses)} адресов.")
    except Exception as e:
        logging.error(f"Ошибка при обновлении балансов: {e}")
    finally:
        cursor.close()


def calculate_totals(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    input_totals = {}
    output_totals = {}

    for item in data:
        address = item['address']
        value = item['value']
        category = item['category']

        if category == 'input':
            input_totals[address] = input_totals.get(address, 0) + value
        elif category == 'output':
            output_totals[address] = output_totals.get(address, 0) + value

    logging.info(f"Рассчитаны итоги: {len(input_totals)} входящих адресов, {len(output_totals)} исходящих адресов.")
    return {"input_totals": input_totals, "output_totals": output_totals}


def execute_query(api_config: APIConfig, query: str, variables: Dict[str, Any]) -> Any:
    max_retries = 10  # Максимальное количество попыток
    retry_delay = 5  # Задержка между попытками в секундах
    global REQUEST_COUNT
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(api_config.url, json={"query": query, "variables": variables},
                                     headers=api_config.headers)

            if response.status_code == 200:
                REQUEST_COUNT += 1
                logging.info(f"Запрос выполнен успешно на попытке {attempt}.")
                return response.json()
            elif response.status_code == 402:  # Лимит запросов исчерпан
                logging.warning(f"Попытка {attempt}: Лимит запросов исчерпан. Переключаем API-ключ.")
                if not api_config.switch_to_next_key():
                    logging.critical("Все API-ключи исчерпаны. Остановка программы.")
                    return None
            else:
                logging.error(f"Попытка {attempt}: Ошибка запроса: {response.status_code}, {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Попытка {attempt}: Ошибка соединения: {e}")

        # Если запрос не удался, подождать перед следующей попыткой
        if attempt < max_retries:
            logging.info(f"Попытка {attempt} не удалась. Повтор через {retry_delay} секунд.")
            time.sleep(retry_delay)
        else:
            logging.critical("Максимальное количество попыток исчерпано. Остановка программы.")

    return None  # Если после всех попыток запрос не удался


def fetch_bitcoin_hashes(api_config: APIConfig, limit: int, offset: int, from_time: str,
                            till_time: str) -> List[Dict[str, str]]:
    query = """
    query ($network: BitcoinNetwork!, $limit: Int!, $offset: Int!, $from: ISO8601DateTime, $till: ISO8601DateTime) {
      bitcoin(network: $network) {
        transactions(
          options: {desc: [\"block.height\", \"index\"], limit: $limit, offset: $offset}
          time: {since: $from, till: $till}
        ) {
          block {
            timestamp {
              time(format: \"%Y-%m-%d %H:%M:%S\")
            }
            height
          }
          inputValue
          input_value_usd: inputValue(in: USD)
          outputCount
          inputCount
          index
          hash
          feeValue
          fee_value_usd: feeValue(in: USD)
        }
      }
    }
    """
    variables = {
        "network": api_config.network,
        "limit": limit,
        "offset": offset,
        "from": from_time,
        "till": till_time
    }
    data = execute_query(api_config, query, variables)
    if not data:
        logging.critical("Данные для транзакций не получены. Остановка выполнения.")
        exit(1)  # Прерывание программы
    try:
        transactions = data["data"]["bitcoin"]["transactions"]
        return [
            {"hash": tx["hash"], "timestamp": tx["block"]["timestamp"]["time"]}
            for tx in transactions
        ]
    except KeyError as e:
        logging.error(f"Ошибка в структуре данных ответа API: {e}")
        return []

def fetch_input_addresses(api_config: APIConfig, tx_hash: str) -> List[Dict[str, Any]]:
    query = """
    query ($hash: String!) {
      bitcoin {
        inputs(txHash: {is: $hash}) {
          inputAddress {
            address
            annotation
          }
          value
        }
      }
    }
    """
    variables = {"hash": tx_hash}
    data = execute_query(api_config, query, variables)
    if data:
        try:
            inputs = [
                {"address": inp["inputAddress"]["address"], "value": inp["value"]}
                for inp in data["data"]["bitcoin"]["inputs"]
            ]
            logging.info(f"Получено {len(inputs)} входящих адресов для транзакции {tx_hash}.")
            return inputs
        except KeyError as e:
            logging.error(f"Ошибка в структуре данных ответа API для входящих адресов транзакции {tx_hash}: {e}")
    else:
        logging.warning(f"Нет данных от API для входящих адресов транзакции {tx_hash}.")
    return []

def fetch_output_addresses(api_config: APIConfig, tx_hash: str) -> List[Dict[str, Any]]:
    query = """
    query ($hash: String!) {
      bitcoin {
        outputs(txHash: {is: $hash}) {
          outputAddress {
            address
            annotation
          }
          value
        }
      }
    }
    """
    variables = {"hash": tx_hash}
    data = execute_query(api_config, query, variables)
    if data:
        try:
            outputs = [
                {"address": out["outputAddress"]["address"], "value": out["value"]}
                for out in data["data"]["bitcoin"]["outputs"]
            ]
            logging.info(f"Получено {len(outputs)} исходящих адресов для транзакции {tx_hash}.")
            return outputs
        except KeyError as e:
            logging.error(f"Ошибка в структуре данных ответа API для исходящих адресов транзакции {tx_hash}: {e}")
    else:
        logging.warning(f"Нет данных от API для исходящих адресов транзакции {tx_hash}.")
    return []


def main() -> None:
    with psycopg2.connect(**DB_SETTINGS) as conn:
        iterations = 61
        for i in range(iterations):
            logging.info(f"Запуск {i + 1} из {iterations}")
            start_time = time.time()
            # Получаем транзакции
            hashes_with_timestamps = fetch_bitcoin_hashes(api_config, LIMIT, OFFSET, FROM_TIME, TILL_TIME)
            if not hashes_with_timestamps:
                print("Нет новых транзакций.")
                break
            all_data = []
            for tx in hashes_with_timestamps:
                tx_hash = tx["hash"]
                timestamp = tx["timestamp"]

                # Получаем input-адреса и output-адреса
                input_addresses = fetch_input_addresses(api_config, tx_hash)
                output_addresses = fetch_output_addresses(api_config, tx_hash)

                for inp in input_addresses:
                    all_data.append({
                        "hash": tx_hash,
                        "address": inp["address"],
                        "category": "input",
                        "value": inp["value"],
                        "timestamp": timestamp
                    })

                for out in output_addresses:
                    all_data.append({
                        "hash": tx_hash,
                        "address": out["address"],
                        "category": "output",
                        "value": out["value"],
                        "timestamp": timestamp
                    })

            if all_data:
                insert_data_to_db(conn, all_data)
                totals = calculate_totals(all_data)  # Подсчёт входящих/исходящих
                update_balances_incremental(conn, totals["input_totals"], totals["output_totals"])  # Обновление базы
                print(f"Всего обработано {len(all_data)} записей.")
            else:
                print("Нет данных для записи в базу.")

            end_time = time.time()
            elapsed_time = end_time - start_time
            sleep_time = max(0, 600 - elapsed_time)
            if i < iterations - 1:
                print(f"Ожидание {sleep_time:.2f} секунд перед следующей проверкой...")
                time.sleep(sleep_time)
            print(f"Цикл завершен. Затрачено времени: {elapsed_time:.2f} секунд.")


if __name__ == "__main__":
    logging.info("Начало выполнения программы.")
    try:
        main()
    except Exception as e:
        logging.critical(f"Критическая ошибка: {e}")
    finally:
        logging.info(f"Завершение выполнения программы. Всего выполнено запросов: {REQUEST_COUNT}")


