from typing import Optional

from pydantic import BaseModel
import yaml


class Proxy(BaseModel):
    proxy_host: str
    proxy_port: int
    proxy_user: str
    proxy_pass: str


class Config(BaseModel):
    proxy: Optional[Proxy]
    oauth2client_service_account_file: str
    google_url: str

    redmine_host: str
    redmine_db: str
    redmine_user: str
    redmine_password: str
    redmine_role: str


def get_config(yaml_file_name: str) -> Config:
    """
    Получить конфигурацию для приложения
    :yaml_file_name: Путь до конфига приложения
    :return: Config
    """

    with open(yaml_file_name, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return Config(**data)


# Test read config
if __name__ == '__main__':
    config = get_config('../config.yaml')
    print(config)
