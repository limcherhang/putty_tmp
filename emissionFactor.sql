CREATE SCHEMA emissionFactor;

CREATE TABLE emissionFactor.`eletricityCompany` (
  `languageId` smallint NOT NULL,
  `English` varchar(50) DEFAULT NULL,
  `Chinese` varchar(30) DEFAULT NULL,
  `Japanese` varchar(30) DEFAULT NULL,
  `Thai` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`languageId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE emissionFactor.`emissionAsset` (
  `EFId` int NOT NULL,
  `companyId` varchar(45) DEFAULT NULL,
  `sourceOfEmission` varchar(220) DEFAULT NULL,
  `vehicleType` varchar(75) DEFAULT NULL,
  `fuelEfficiency` float DEFAULT NULL,
  `co2Factor` float DEFAULT NULL,
  `ch4Factor` float DEFAULT NULL,
  `n2oFactor` float DEFAULT NULL,
  `ar4Factor` float DEFAULT NULL,
  `ar5Factor` float DEFAULT NULL,
  `baseUnit` varchar(30) DEFAULT NULL,
  `source` varchar(30) DEFAULT NULL,
  `urlName` varchar(55) DEFAULT NULL,
  `sheetName` varchar(45) DEFAULT NULL,
  `file` varchar(100) DEFAULT NULL,
  `link` varchar(130) DEFAULT NULL,
  `year` smallint DEFAULT NULL,
  `emissionUnit` varchar(20) DEFAULT NULL,
  `ch4Unit` varchar(20) DEFAULT NULL,
  `n2oUnit` varchar(20) DEFAULT NULL,
  `sourceType` varchar(50) DEFAULT NULL,
  `sId` smallint DEFAULT NULL,
  PRIMARY KEY (`EFId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE emissionFactor.`mongoInfo` (
  `sId` int NOT NULL,
  `_id` varchar(45) DEFAULT NULL,
  `type` varchar(70) DEFAULT NULL,
  `scope` varchar(10) DEFAULT NULL,
  `typeOfEmission` varchar(70) DEFAULT NULL,
  `category` varchar(70) DEFAULT NULL,
  `categoryKey` varchar(20) DEFAULT NULL,
  `nameKey` varchar(20) DEFAULT NULL,
  `sourceType` varchar(20) DEFAULT NULL,
  `usedRegion` varchar(20) DEFAULT NULL,
  `sourceOfEmission` varchar(45) DEFAULT NULL,
  `EFSId` smallint DEFAULT NULL,
  `languageId` smallint DEFAULT NULL,
  PRIMARY KEY (`sId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE emissionFactor.`mongoInfo` (
  `sId` int NOT NULL,
  `_id` varchar(45) DEFAULT NULL,
  `type` varchar(70) DEFAULT NULL,
  `scope` varchar(10) DEFAULT NULL,
  `typeOfEmission` varchar(70) DEFAULT NULL,
  `category` varchar(70) DEFAULT NULL,
  `categoryKey` varchar(20) DEFAULT NULL,
  `nameKey` varchar(20) DEFAULT NULL,
  `sourceType` varchar(20) DEFAULT NULL,
  `usedRegion` varchar(20) DEFAULT NULL,
  `sourceOfEmission` varchar(45) DEFAULT NULL,
  `EFSId` smallint DEFAULT NULL,
  `languageId` smallint DEFAULT NULL,
  PRIMARY KEY (`sId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
