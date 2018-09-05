# cassandra2pgsql

> Copy table data from Cassandra to Postgre

**KULLANIM:**

 - Cassandra ve PostgreSQL bağlantı bilgileri app.js içinden tanımlanır
- Parametreler : 
	- [kaynak tablo ismi] 
	- [hedef tablo ismi] 
	- [kaynak tablodaki primary key alan ismi] 
	- copypk
- Tablolar arasında primary key ile igili işlem yoksa;
  -	node app.js [kaynak tablo ismi]  [hedef tablo ismi] 
- Kaynak tablodaki "primary key" alanı aktarılmak **istenmiyorsa**;
	- node app.js [kaynak tablo ismi]  [hedef tablo ismi]  [kaynak tablodaki primary key alan ismi] 
- Kaynak tablodaki "primary key" alanı aktarılmak **isteniyorsa**
	- node app.js [kaynak tablo ismi]  [hedef tablo ismi]  [kaynak tablodaki primary key alan ismi] [**copypk**]

Uygulama hazırladığı sql içeriğini uygulamanın çalıştığı dizin içinde "[kaynaktabloismi]2[hedeftablosismi].sql" formatında dosya olarak kaydeder.
