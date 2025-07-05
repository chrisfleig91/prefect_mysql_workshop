BEGIN TRANSACTION;
  ALTER TABLE Kunde ADD COLUMN Quellsystem VARCHAR(30);

  UPDATE Kunde SET Quellsystem = 'e-com_kunden.csv' WHERE KundenID like "1%";
  UPDATE Kunde SET Quellsystem = 'bestandskunden.xlsx' WHERE KundenID like "2%";
  UPDATE Kunde SET Quellsystem = 'crm_kunden.json' WHERE KundenID like "3%";

COMMIT TRANSACTION;