module.exports  = {
    gPrefixCol:{
        c2p:{
            testManagers: 'mn',
            testCustomers: 'cs',
            testContracts: 'cn',
            testBills: 'bl'
        },
        p2c:{
            'mn': 'testManagers',
            'cs': 'testCustomer',
            'cn': 'testContracts',
            'bl': 'testBills'
        }
    },
    gPath: {
        manPath: [
            [ 'testManagers', '_id'],
            [ 'testCustomers', 'm_id'],
            [ 'testContracts', 'c_id'],
            [ 'testBills', 'contr_id']

        ],
        manPath2: [
            [ 'testManagers', '_id'],
            [ 'testCustomers', 'm_id2']

        ]
    },
    gFieldDepPath: {
        testContracts: {
            c_id: ['manPath','testCustomers']
        },
        testBills: {
            contr_id: ['manPath','testContracts']
        }
    },
    gBackRef: {
        testManagers:[
            ['testCustomers', 'm_id']
        ],
        testCustomers: [
            ['testContracts', 'c_id']
        ],
        testContracts: [
            ['testBills', 'contr_id']
        ]
    },
    gDepScore: {
        testCustomers:{
            name: {
                testManagers: 'name'
            }
        }
    },
    gScore: {
        testManagers:{
            name: 1
        }
    },
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
    },
    gDepScoreJoin:{
        testManagers:{
            testCustomers: {
                m_id:1
            },
            testContracts: {
                c_id:1
            },
            testBills: {
                contr_id: 1
            }
        }
    },
    gArrayFields:{
        testCustomers: {m_id:1, m_id2:1},
        testContracts:{ c_id: 1},
        testBills: {contr_id:1}
    },
    gWordFields:{
        testCustomers: {note:1}
    }
}
