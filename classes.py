from typing import Hashable, Any


class TransactionError(Exception):
    pass

'''
Необходимо реализовать интерфейс базы данных. В ней необходимо поддерживать вложенные транзакции.
В задании не указаны, какие именно должны быть внутренние транзакции, поэтому были сделаны следующие предположения:
1) Вложенные транзакции зависимы от внешних.
2) Внутренние транзакции имеют больший приоритет, чем внешние. Так, например, при изменении одной переменной и во внешней
   и во внутренней транзакции, COMMIT обеих транзакций установит в БД значение внутренней транзакции.
3) CRUD методы работают так, как будто все транзакции приняты (коммитнуты).

Данная логика основывается на простом предположении, что транзакции заведомо не буду сделаны ложными,
и цель их создания - принятие их изменений.

Данная имитация БД похожа на Redis, однако в отличие от него поддерживает вложенные транзакции, с описанной выше логикой.
Реализованные транзакции поддерживают практически все принципы ACID.
Атомарность реализована через отлов ошибки TransactionError.
Согласованность может быть реализована через сохранение значений в bytes (как это делает Redis).

Однако, вложенные зависимые транзакции (субтранзакции) нарушают принцип устойчивости, так как внутрення транзакция может быть принята,
однако при откате внешней транзакции данные не сохранятся.
'''

class DBImitate:

    def __init__(self) -> None:
        self.db: dict = dict()
        self.transaction_log: list = [self.db]  # Лог транзакции представляет собой стек из записей транзакций. Первоначально содержит БД
        # [{A: [0, 1], B: [1, 2]}, {C: 3}]

    def set(self, key: Hashable, value: Any) -> None:
        """Cохраняет аргумент в базе данных

        Args:
            key (Hashable): Имя переменной
            value (Any): Значение переменной
        """

        # При добавлении / обновлении значения переменной есть три случая

        if len(self.transaction_log) == 1:  # 1 случай. Транзакции нет. Добавляем значение в БД
            self.transaction_log[-1][key] = value
        elif len(self.transaction_log) > 2:  # 2 случай. Одна транзакция. В лог транзакции добавляется [значение из бд, новое значение]
            if key in self.transaction_log[-2]:  # Реализация Update
                self.transaction_log[-1][key] = [self.transaction_log[-2][key][1], value]
            else:
                self.transaction_log[-1][key] = [None, value]
        else:
            self.transaction_log[-1][key] = [self.transaction_log[-2].get(key, None), value] # 3 случай. Добавляется вложенная транзакция

    def get(self, key: Hashable) -> str | bytes:
        """Возвращает, ранее сохраненную переменную. Если такой переменной
           не было сохранено, возвращает NULL

        Args:
            key (Hashable): Искомая переменная

        Returns:
            str | bytes: Значение переменной или NULL
        """

        if len(self.transaction_log) == 1:
            return self.db.get(key, 'NULL')
        for transaction in reversed(self.transaction_log):
            if key in transaction:
                if isinstance(transaction[key], list):
                    if transaction[key][1] is not None:
                        return transaction[key][1]
                    return 'NULL'
                return transaction[key]
        return 'NULL'

    def unset(self, key: Hashable) -> None:
        """Удаляет, ранее установленную переменную. Если значение не было
           установлено, не делает ничего

        Args:
            key (Hashable): Переменная для удаления
        """
        # Аналогично добавлению get
        if len(self.transaction_log) == 1:
            if key in self.db:
                self.transaction_log[-1].pop(key)
        else:
            if key in self.transaction_log[-1]:
                self.transaction_log[-1].pop(key)
            else:
                if key in self.db:
                    self.transaction_log[-1][key] = [self.db[key], None]


    def find(self, value: Any) -> list[str]:
        """Выводит найденные установленные переменные для искомого значения

        Args:
            value (Any): Искомое значение

        Returns:
            list[str]: Переменные для искомого значения
        """
        # Значения ищутся из логики 3 пункта
        keys = set()
        for transaction in reversed(self.transaction_log):
            for key in transaction:
                if (isinstance(transaction[key], list) and transaction[key][1] == value) or transaction[key] == value:
                    if self.get(key) == value: # Проверка на текущее значение переменной (текущее значение - из последней транзакции, где переменная меняется)
                        keys.add(key)
        return list(keys)[::-1]

    def counts(self, value: str | bytes) -> int:
        """Показывает сколько раз данные значение встречается в базе данных

        Args:
            value (_type_): Искомое значение

        Returns:
            int: Количество переменных с этим значением
        """
        return len(self.find(value))

    def begin_transaction(self):
        """
        Начало транзакции
        """
        self.transaction_log.append(dict())

    def commit_transaction(self):
        """
        Фиксация изменений текущей (самой внутренней) транзакции
        """
        # случаи аналогично добавлению.
        if len(self.transaction_log) > 1:
            try:
                for key in self.transaction_log[-1]:
                    if self.transaction_log[-1][key][1] is None: # в транзакции удаляется значение
                        if key in self.transaction_log[-2]: # в прошлой транзакции было изменение этой переменной
                            self.transaction_log[-2].pop(key)
                        else:
                            self.transaction_log[-2][key] = self.transaction_log[-1][key] # в прошлой транзакции не было изменения, добавляется изменение в предыдущую транзакцию
                    else: # в транзакции меняется или создается значение
                        if len(self.transaction_log) == 2: # верхняя транзакция. Значение добавляется в БД
                            self.db[key] = self.transaction_log[-1][key][1]
                        else:
                            self.transaction_log[-2][key] = self.transaction_log[-1][key] # перенос значения в прошлую транзакцию
                self.transaction_log.pop()
            except TransactionError:
                self.rollback_transaction()


    def rollback_transaction(self):
        """
        Откат текущей (самой внутренней) транзакции
        """
        if len(self.transaction_log) > 1: # есть транзакции
            for key in self.transaction_log[-1]:
                if self.transaction_log[-1][key][0] is not None:  # в транзакции изменяется переменная
                    if len(self.transaction_log) == 2: # верхняя транзакция
                        self.transaction_log[-2][key] = self.transaction_log[-1][key][0]
                    else:
                        self.transaction_log[-2][key][1] = self.transaction_log[-1][key][0]
            # при откате удаления или создания переменной ничего не происходит в бд
            self.transaction_log.pop()
