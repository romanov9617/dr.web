from classes import DBImitate

from sys import stdin


def main():
    db_imitate = DBImitate()
    while line := stdin.readline().strip():
        command, *args = line.split()
        match command:

            case 'END':
                return

            case 'SET':
                key, value = args
                db_imitate.set(key, value)

            case 'GET':
                key = args[0]
                print(db_imitate.get(key))

            case 'UNSET':
                key = args[0]
                db_imitate.unset(key)

            case 'FIND':
                value = args[0]
                print(' '.join(db_imitate.find(value)))

            case 'COUNTS':
                value = args[0]
                print(db_imitate.counts(value))

            case 'BEGIN':
                db_imitate.begin_transaction()

            case 'ROLLBACK':
                db_imitate.rollback_transaction()

            case 'COMMIT':
                db_imitate.commit_transaction()

            case _:
                print('Неизвестная команда')

    return
