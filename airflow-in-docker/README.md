Друзья, пристальный взгляд на запуск Airlow под Windows говорит о том, что проще всего и универсально использовать Docker.

Другие варианты либо не везде работают (например, в Windows 7 нет WSL), либо слишком долго надо исправлять сами исходники Airflow (для нативного запуска).

Поэтому держите инструкцию, по которой я сейчас успешно прошел.

1. Установить PowerShell (https://github.com/PowerShell/PowerShell#get-powershell).
2. Установить Docker и перезапустить систему https://desktop.docker.com/win/stable/Docker%20Desktop%20Installer.exe
3. Запустить PowerShell и выполнить следующие команды:
   1. git clone https://github.com/rails-to-cosmos/geekbrains-airflow-materials
   2. cd geekbrains-airflow-materials/airflow-in-docker
   3. docker-compose -f docker-compose-LocalExecutor.yml up

Через полминуты проверить http://localhost:8080/admin/

Должна быть запущена стабильная версия 1.10.9.
