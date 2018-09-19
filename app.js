/***** KULLANIM *****
 *************************
 * BASLANGIC PARAMETRELERI
 *************************
 * node app.js 
 * [kaynak tablo ismi] 
 * [hedef tablo ismi]
 * [kaynak tablo primary key column]
 * copypk --> primary key kolon degerlerin aktarilmak isteniyorsa yazilmalidir
 */

/*
  packets
 */
var cassandra = require('cassandra-driver')
var {
  Pool,
  Client
} = require('pg')
var fs = require('fs')
var path = require('path')

let readline = require('readline')
var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

/*
  database connection stringa
 */
let cass_client = new cassandra.Client({
  contactPoints: ["0.0.0.0"],
  keyspace: ''
})

const postg_client = new Client({
  connectionString: 'postgresql://dbuser:secretpassword@database.server.com:5432/mydb',
})
postg_client.connect()


/*
  variables
 */
let
  args = process.argv,
  source_table = args[2],
  target_table = args[3],
  pkey_column = args[4],
  pkey_value_import = args[5],
  source_columns = [],
  target_columns = [],
  column_list = [],
  insert_cmd = "",
  pkey_value_import_control = "copypk"

/*
  functions
 */

/**
 * kaynak tablonun kolonlarini alir
 * @return {[type]} [description]
 */
let get_source_columns = () => {
  return new Promise((fulfill, reject) => {
    let cmd = `SELECT column_name FROM system_schema.columns  WHERE table_name='${source_table}' ALLOW FILTERING`
    cass_client.execute(cmd, (err, res) => {
      if (err) {
        reject(err.stack)
      } else {
        if (res.rowLength == 0) {
          console.log("Kaynak tablo bulunamadı")
          reject('')
        }
        res.rows.forEach((row) => {
          source_columns.push(row.column_name)
        })
        console.log("SOURCE : ", source_columns)
        fulfill('')
      }
    })
  })
}

/**
 * hedef tablonun kolonlarini alir
 * @return {[type]} [description]
 */
let get_target_columns = () => {
  return new Promise((fulfill, reject) => {
    let cmd = `SELECT column_name FROM information_schema.columns WHERE table_name = '${target_table}'`
    postg_client.query(cmd, (err, res) => {
      if (err) {
        reject(err.stack)
      } else {
        if (res.rowCount == 0) {
          console.log("Hedef tablo bulunamadı")
          reject('')
        }
        res.rows.forEach((row) => {
          target_columns.push(row.column_name)
        })
        console.log("TARGET :", target_columns)
        fulfill('')
      }
    })
  })
}

/**
 * kaynak tablodaki kolon hedef tabloda var mi kontrol edilir
 * mevcut degilse kullanicidan hangi kolona kaydedilecegi alinir
 * @param  {[type]} s_col [kontrol edilecek kaynak tablo kolonu]
 * @return {[type]}       [kullanicinin girdigi deger geri dondurulur]
 */
let is_define = (s_col) => {
  return new Promise((fulfill, reject) => {
    if (target_columns.indexOf(s_col) == -1) {
      rl.question(`'${s_col}' kolonu hangi kolona kaydedilsin ?`, (answer) => {
        if (answer == "") {
          fulfill('')
        } else {
          if (target_columns.indexOf(answer) == -1) {
            console.log(`Hedef tabloda '${answer}' isminde kolon bulunmuyor. Tekrar giriniz`)
            control_columns(s_col)
          } else {
            column_list.push(`${s_col},${answer}`)
            fulfill('')
          }
        }
      })
    } else {
      column_list.push(`${s_col},${s_col}`)
      fulfill('')
    }
  })
}

/**
 * kaynak tablo kolonlari hedef tablo kolonlarinda var mi kontrol ediliyor
 * @type {[type]}
 */
let src = source_columns
let control_columns = (item) => {
  is_define(item).then(() => {
    if (src.length > 0) {
      control_columns(src.shift())
    } else {
      console.log(column_list)
      start_import()
    }
  })
}

/**
 * kaynak tablodaki tum kayitlar cekiliyor
 * insert into komutu olusturuluyor
 * @return {[type]} [description]
 */
let get_rows = () => {
  return new Promise((fulfill, reject) => {
    let cmd = `SELECT * FROM ${source_table}`
    cass_client.execute(cmd, (err, res) => {
      if (err) {
        reject(err.stack)
      } else {
        if (res.rowLength == 0) {
          console.log("Kaynak tabloda data bulunamadı")
          reject('')
        }

        // insert kolonlari
        let insert_cols = ""
        column_list.forEach((col) => {
          cols = col.split(',')
          insert_cols += `${cols[1]},`
        })

        if (pkey_value_import_control != pkey_value_import) {
          insert_cols = insert_cols.replace("," + pkey_column, "")
          insert_cols = insert_cols.replace(pkey_column + ",", "")
        }

        insert_cmd += `INSERT INTO ${target_table} (${insert_cols.substring(0, insert_cols.length-1)}) VALUES `
        // data cekildi
        res.rows.forEach((row) => {
          // insert values
          let insert_vals = ""
          let insert_value = ""
          let insert_column = ""
          column_list.forEach((col) => {
            cols = col.split(',')
            insert_column = cols[0]
            insert_value = row[insert_column]

            // primary key kolonundaki degerler alinmali mi kontrol eidliyor
            if (insert_column == pkey_column && pkey_value_import_control == pkey_value_import) {
              insert_vals += `'${insert_value}',`
            } else {
              if (insert_column != pkey_column) {
                if (insert_value == null) {
                  insert_vals += `null,`
                } else {
                  // tarih degerinin formatliyor
                  if (JSON.stringify(insert_value).indexOf("000Z") > -1) {
                    insert_value = new Date(insert_value).toISOString()
                  }

                  insert_vals += `'${insert_value}',`
                }
              }
            }
          })

          insert_cmd += `(${insert_vals.substring(0, insert_vals.length-1)}),`
        })
        insert_cmd = insert_cmd.substring(0, insert_cmd.length - 1)

        fulfill(insert_cmd)
      }
    })
  })
}

/**
 * insert into komutu calistiriliyor
 * @param  {[type]} insert_cmd [description]
 * @return {[type]}            [description]
 */
let insert_rows = (insert_cmd) => {
  return new Promise((fulfill, reject) => {
    // insert cumlesi dosyaya kaydediliyor    
    let app_dir = path.dirname(require.main.filename);
    let file_name = `/bh_${source_table}2${target_table}.sql`
    fs.writeFile(app_dir + file_name, insert_cmd, function(err) {
      if (err) {
        console.log(err);
      }
      console.log("SQL içerik dosyaya kaydedildi");
    });

    postg_client.query(insert_cmd, (err, res) => {
      if (err) {
        reject(err)
      } else {
        //console.log("CMD :", insert_cmd)
        console.log(res)
        fulfill('')
      }
    })
  })
}

/*
  main functions
 */

get_source_columns().then(() => {
  return get_target_columns()
}).then(() => {
  console.log("Aktarılmasını istemediğiniz kolonu boş geçiniz")
  control_columns(src.shift())
}).catch(err => {
  rl.close()
  console.error(err)
})

/**
 * kolon kontrolleri yapildiktan ve gerekli bilgiler alindiktan sonra
 * hedef tabloya data akatarma islemi baslatilir
 * @return {[type]} [description]
 */
let start_import = () => {

  rl.question("Hedef tablo boşaltılsın mı? E/H", (answer) => {
    if (answer == "E" || answer == "") {
      insert_cmd = `DELETE FROM ${target_table}; `
    }

    rl.question("Datalar aktarılsın mı? E/H", (answer) => {
      if (answer == "E" || answer == "") {
        return get_rows().then((cmd) => {
          rl.close()
          return insert_rows(cmd)
        }).catch(err => {
          rl.close()
          //console.log(insert_cmd)          
          console.error(err)
        })
      } else {
        rl.close()
        console.log("Selametle, Kolay Gelsin...")
      }
    })
  })
}