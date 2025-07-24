TotalBU = SUMX('Postion Contract Detail', 'Postion Contract Detail'[Remaining_BU])

Bushel Balance = SUM('Open Contracts'[ISSUED_BU]) - SUM('Open Contracts'[REMAINING_BU])