Rabbit - v.5
=====
## Запуск теста
```
nodeunit test.js
```

## init( config , callback )
Функци инициализации и подключения к БД + запускает Rabbit
* config - конфиг,
    * config.mongo - конфиг mongo,
    * config.redis - конфиг редис,
    * config.gPrefixCol - справочник префиксов путей;

    ```
    gPrefixCol: {
        c2p: {
            managers: 'mn',
            customers: 'cs',
            contracts: 'cn',
            bills: 'bl'
        },

        p2c: {
            mn: 'managers',
            cs: 'customers',
            cn: 'contracts',
            bl: 'bills'
        }
    }
    ```

    * config.gPath - справочник всех путей;

    ```
     gPath: {
            manPath: [
                [ 'managers', '_id'],
                [ 'customers', 'm_id'],
                [ 'contracts', 'c_id'],
                [ 'bills', 'contr_id']

            ]
        }
    ```

    * config.gFieldDepPath - справочник связи названия поля и пути;

    ```
    gFieldDepPath: {
            contracts: {
                c_id: ['manPath','customers']
            },
            bills: {
                contr_id: ['manPath','contracts']
            }
    }
    ```

    * gBackRef - справочник колекций связанных с документов (для проверки перед удалением)

    ```
        gBackRef: {
               managers:[
                   ['customers', 'm_id']
               ]
           }
    ```

    * gScore - справочник колекций где есть поле с весом

    ```
    gScore: {
             testManagers:{
                 name: 1
             }
         }
    ```

    * gScoreJoin - справочник колекций в которых есть внешние поля с весом

    ```
    gScoreJoin:{
             testCustomers:{
                 m_id: {
                     testManagers:1
                 }
             },
             testContracts:{
                 c_id: {
                     testManagers:1
                 }
             },
             testBills:{
                 contr_id: {
                     testManagers:1
                 }
             }
         }
    ```

    * gWordFields - справочник полей разбиваемых на слова для поиска

    ```
    gWordFields:{
            testCustomers: {note:1}
        }
    ```

    * gArrayFields - справочник с полями массивами в колекции

    ```
     gArrayFields:{
             testCustomers: {m_id:1},
             testContracts:{ c_id: 1},
             testBills: {contr_id:1}
         }
    ```
* callback (err ,res)


## open ( port, host, callback )
Запускает Rabbit

## close (callback )
Закрывает Rabbit
