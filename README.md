WIP

Aim of this project is to improve the runtime of multiple parallel backtests that take my laptop too long to run.

This project is about creating a high-performance infrastructure for large backtests, it is not about finding a great trading strategy. For this project, I am using simple ma crossover backtest on crypto data.

I use crypto as a data source because it is freely available. The data can be replaced with other trading data anytime.

Running multiple backtests on my laptop:
Took me 15 minutes to backtest all "strategies".

<img width="519" height="663.5" alt="image" src="https://github.com/user-attachments/assets/c69a2e32-5c29-45cb-a07d-1e817d816908" />


How to deploy this project:
1) Clone repo
2) Configure secrets in settigs:
    AZURE_CREDENTIALS
    POSTGRES_NAME
    POSTGRES_PASSWORD
    SSH_PUBLIC_KEY
    STORAGE_ACCOUNT_NAME (only lowercase letter)
    VM_NAME
