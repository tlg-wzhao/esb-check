



/*
Version | Date        | Author    | Ref         | Description 
------- | ----------- | --------- | ----------- | -----------
1       | 2021-08-12  | MatteoC   | TLGESB-791  | initial
2       | 2021-10-05  | MatteoC   | TLGESB-732  | MAU generation
3       | 2021-10-12  | MatteoC   | TLGESB-866  | add RealmId
4       | 2022-09-29  | PaoloBa   | TLGESB-1090 | add process Fornasetti
5       | 2022-10-03  | SimoneB   | TLGESB-1090 | Fornasetti must be processed even with locations set as targetOperation = 'Filter' in GlobalQueueRoutingConfig
6       | 2023-02-13  | SimoneB   | TLGESB-1265 | added brandId and insert into DGActionsProcess
7       | 2023-08-22  | ElenaG    | TLGESB-1479 | added insert into IntBillShipStatusUpdNotificationProcess for LogisticStoreReturn
8       | 2023-09-29  | AntonioC  | TLGESB-1558 | isPickup to be considered for ShipmentNotificationPartner flow
9       | 2023-10-13  | ElenaG	  | TLGESB-1494 | [farfetchCode] -> [brand]
*/

CREATE PROCEDURE [dbo].[KiboNotificationInsert_CP_InsertProcesses]
(
	@notificationId UNIQUEIDENTIFIER,
	@date DATETIME,	
	@stateFromCode VARCHAR(50),
	@stateToCode VARCHAR(50),
	@locationID VARCHAR(20),
	@shipmentID VARCHAR(20),
	@orderId VARCHAR(20),
	@externalStoreID VARCHAR(30),
	@externalOrderID VARCHAR(30),
	@brandId INT,
	@jsonResponse NVARCHAR(MAX) OUT,
	@isPickup VARCHAR(30) --TLGESB-1558
) 
AS
BEGIN

	DECLARE @statusToProcess INT = dbo.StatusToProcess();
	DECLARE @statusPartiallyInsertedOnStaging INT = dbo.StatusPartiallyInsertedOnStaging();

	DECLARE @successForshipmentNotificationCustomerDataPlatform BIT = 0;
	
	--TLGESB-1265
	IF EXISTS (
		SELECT TOP 1 1
		FROM
			[GlobalQueueRoutingConfig]
		WHERE
			brandId = @brandId
			AND sourceFlowName = 'KiboNotificationInsert_CP'
			AND targetFlowName = 'lapp-dg-orderupdate'
	)
	BEGIN
		INSERT INTO dbo.DGActionsProcess 
		(
			sourceFlowName,
			sourceFlowKey,
			sourceFlowKeyDescription,
			targetOperation,
			applicationSystemId,
			brandId,
			externalOrderId,
			sourceFlowProcessKey,
			transactionStatus,
			transactionDate,
			creationDate,
			retryCount,
			isDeleted
		)
		VALUES
		(
			'KiboNotificationInsert_CP',
			@orderId,
			'orderID',
			'lapp-dg-orderupdate',
			'DGMS',
			@brandId,
			@externalOrderID,
			@notificationId,
			@statusPartiallyInsertedOnStaging,
			@date,
			@date,
			0,
			0
		)
	END

	--TLGESB-1090
	IF @stateToCode = '500' AND EXISTS (
		SELECT TOP 1 1
		FROM 
			[Location]
		WHERE
			[Location].[shipmentNotificationCustomerDataPlatform] = 1
			AND ISNULL(locationId, @locationID) = @locationID
	)
	BEGIN

		INSERT INTO IntBillShipStatusUpdNotificationProcess
		(
			shipStatusUpdNotificationId,
			targetSystem,
			targetOperation,
			transactionStatus,
			transactionDate,
			retryCount
		)
		SELECT 
			@notificationId,
			'azureLogicApp',
			'FornasettiFulfilledOrders',
			@statusToProcess,
			@date,
			0;
		SET @successForshipmentNotificationCustomerDataPlatform = 1;
	END
	---------------

	IF EXISTS 
		(
			SELECT TOP 1
				1
			FROM
				GlobalQueueRoutingConfig WITH (NOLOCK)
			WHERE
				sourceFlowName = 'KiboNotificationInsert'
				AND targetFlowName = 'IntOrdShipStatusOrderStore'
				AND targetOperation = 'Filter'
				AND ISNULL(locationId, @locationID) = @locationID
		)
	BEGIN
		IF @successForshipmentNotificationCustomerDataPlatform = 1
		BEGIN
			SET @jsonResponse = '{"result":"success"}';
		END
		ELSE
		BEGIN
			SET @jsonResponse = '{"result":"discarded"}';
		END
		RETURN 0;
	END;


	IF( -- Ignore Kibo Notification CompletePackage 21 -> 41 and 21 -> 34, must not be processed
		@stateFromCode != '21' AND
		@stateToCode NOT IN ('34','41')
	)
	BEGIN
		INSERT INTO IntBillShipStatusUpdNotificationProcess
		(
			shipStatusUpdNotificationId,
			targetSystem,
			targetOperation,
			realmId, --TLGESB-866
			transactionStatus,
			transactionDate,
			retryCount
		)
		SELECT DISTINCT
			@notificationId,
			GlobalQueueRoutingConfig.applicationSystemId,
			targetOperation,
			realmId, --TLGESB-866
			@statusToProcess,
			@date,
			0
		FROM
			IntBillShipStatusUpdNotification

			INNER JOIN [Location]
			ON IntBillShipStatusUpdNotification.locationID = [Location].locationId

			INNER JOIN KiboOrderHeader --TLGESB-866
			ON KiboOrderHeader.externalOrderID = IntBillShipStatusUpdNotification.externalOrderID
			AND flowName = 'getOrder'
			AND KiboOrderHeader.isDeleted = 0

			INNER JOIN GlobalQueueRoutingConfig
			ON sourceFlowName = 'KiboNotificationInsert'
			AND targetFlowName = 'IntOrdShipStatusOrderStore'
			AND targetOperation = 'Process'
		WHERE
			IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId
			AND [Location].[orderStatusStore] = 1
			AND [Location].[editOrderStatus] = 0;				

		INSERT INTO IntBillShipStatusUpdNotificationProcess
		(
			shipStatusUpdNotificationId,
			targetSystem,
			targetOperation,
			realmId, --TLGESB-866
			transactionStatus,
			transactionDate,
			retryCount
		)
		SELECT DISTINCT
			@notificationId,
			GlobalQueueRoutingConfig.applicationSystemId,
			targetOperation,
			realmId, --TLGESB-866
			@statusToProcess,
			@date,
			0
		FROM
			IntBillShipStatusUpdNotification

			INNER JOIN [Location]
			ON IntBillShipStatusUpdNotification.locationID = [Location].locationId

			INNER JOIN KiboOrderHeader --TLGESB-866
			ON KiboOrderHeader.externalOrderID = IntBillShipStatusUpdNotification.externalOrderID
			AND flowName = 'getOrder'
			AND KiboOrderHeader.isDeleted = 0

			INNER JOIN GlobalQueueRoutingConfig
			ON sourceFlowName = 'KiboNotificationInsert'
			AND targetFlowName = 'IntOrdShipStatusOrderStoreOtherWh'
			AND targetOperation = 'Process'
		WHERE
			IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId
			AND [Location].[orderStatusStore] = 1
			AND [Location].[editOrderStatus] = 1;

		--TLGESB-445
		INSERT INTO IntBillShipStatusUpdNotificationProcess
		(
			shipStatusUpdNotificationId,
			targetSystem,
			targetOperation,
			transactionStatus,
			transactionDate,
			retryCount
		)
		SELECT DISTINCT
			@notificationId,
			applicationSystemId,
			targetOperation,
			@statusToProcess,
			@date,
			0
		FROM
			IntBillShipStatusUpdNotification

			INNER JOIN [Location]
			ON IntBillShipStatusUpdNotification.locationID = [Location].locationId

			INNER JOIN GlobalQueueRoutingConfig
			ON sourceFlowName = 'KiboNotificationInsert'
			AND targetFlowName = 'IntOrdReturnToGeodis'
			AND targetOperation = 'Process'
		WHERE
			IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId
			AND [Location].[returnToGeodis] = 1
	---------------
	/* start TLGESB-1479 */

		INSERT INTO IntBillShipStatusUpdNotificationProcess
		(
			shipStatusUpdNotificationId,
			targetSystem,
			targetOperation,
			transactionStatus,
			transactionDate,
			retryCount
		)
		SELECT DISTINCT
			@notificationId,
			applicationSystemId,
			targetOperation,
			@statusToProcess,
			@date,
			0
		FROM
			IntBillShipStatusUpdNotification

			INNER JOIN [Location]
			ON IntBillShipStatusUpdNotification.locationID = [Location].locationId

			INNER JOIN GlobalQueueRoutingConfig
			ON sourceFlowName = 'KiboNotificationInsert_CP'
			AND targetFlowName = 'LogisticStoreReturn'
			AND targetOperation = 'StoreReturn'
			AND [Location].[wmsReturnLocationId] = [GlobalQueueRoutingConfig].locationId

			INNER JOIN [Location] as ReturnLocation -- verifica se wmsReturnLocationId è presente nella tabella location 
			ON [Location].[wmsReturnLocationId] = [ReturnLocation].locationId
		WHERE
			IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId
			AND [Location].[wmsReturnLocationId] is not null
			--AND [Location].[returnToGeodis] = 1

	/* end TLGESB-1479 */
	END

	---TLGESB-732 MAU file
	INSERT INTO IntOrdShipStatusOrderProcess
	(
		[intOrdShipStatusOrderFileId], 
		[intOrdShipStatusOrderHeaderId], 
		[activityId], 
		[targetSystem], 
		[targetOperation], 
		[orderStatus], 
		[transactionId], 
		[transactionStatus], 
		[transactionDate], 
		[retryCount],
		[realmId],
		[sourceFlowKey]
	)
	SELECT DISTINCT
		[intOrdShipStatusOrderFileId] = NULL, 
		[intOrdShipStatusOrderHeaderId] = NULL, 
		[activityId] = NULL, 
		[targetSystem] = applicationSystemId, 
		[targetOperation], 
		[orderStatus] = NULL,
		[transactionId] = NULL, 
		[transactionStatus] = @statusToProcess, 
		[transactionDate] = @date, 
		[retryCount] = 0,
		[realmId] = NULL,
		[sourceFlowKey] = @notificationId
	FROM
		IntBillShipStatusUpdNotification

		INNER JOIN GlobalQueueRoutingConfig WITH (NOLOCK)
		ON sourceFlowName = 'KiboNotificationInsert'
		AND targetFlowName = 'IntOrdShipStatusOrderProcess'
		AND targetOperation = 'ProcessMAU'
	WHERE
		IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId;

	---------------

	-- TLGESB-796 Process OC
	DECLARE @ToUpdateDocuments BIT = 0;

	DECLARE @Actions AS TABLE(
		targetOperation VARCHAR(50),
		transactionStatus INT
	);
		
	-- Verifico se devo mandare i documenti prima o no
	SELECT 
		@ToUpdateDocuments = CASE WHEN sendShipingLabels = 1 OR sendInvoices = 1 THEN 1 ELSE 0 END
	FROM
		IntBillShipStatusUpdNotification WITH (NOLOCK)

		INNER JOIN Brand WITH (NOLOCK)
		ON LEFT(IntBillShipStatusUpdNotification.externalOrderID, 2) = Brand.brand

		INNER JOIN MarketplaceOrder
		ON MarketplaceOrder.isDeleted = 0
		AND MarketplaceOrder.merchantOrderNumber = IntBillShipStatusUpdNotification.externalOrderID
		AND MarketplaceOrder.channel = 'OC'

		INNER JOIN MarketplaceFlowConfig WITH (NOLOCK)
		ON MarketplaceOrder.marketplace =  MarketplaceFlowConfig.marketplace
		AND MarketplaceOrder.channel =  MarketplaceFlowConfig.channel
		AND MarketplaceFlowConfig.brandId =  Brand.brandId

	WHERE
		IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId;
			
	INSERT INTO @Actions VALUES ('Shipped', CASE WHEN @ToUpdateDocuments = 1 THEN @statusPartiallyInsertedOnStaging ELSE @statusToProcess END);

	IF (@ToUpdateDocuments = 1)
	BEGIN
		INSERT INTO @Actions VALUES ('UpdateDocuments', @statusToProcess);
	END

	--TLGESB-796
	IF NOT EXISTS (
			
				SELECT 
					TOP 1 1
				FROM 
					IntBillShipStatusUpdNotification

					INNER JOIN Brand WITH (NOLOCK)
					ON LEFT(IntBillShipStatusUpdNotification.externalOrderID, 2) = Brand.brand

					INNER JOIN MarketplaceActionsProcess
					ON MarketplaceActionsProcess.isDeleted = 0
					AND MarketplaceActionsProcess.brandId = Brand.brandId
					AND MarketplaceActionsProcess.sourceFlowName = 'ShipStatusOrderStaging'
					AND MarketplaceActionsProcess.targetOperation = 'Shipped'

					INNER JOIN IntShipShipAllocNotification
					ON IntShipShipAllocNotification.shipAllocNotificationId = MarketplaceActionsProcess.sourceFlowProcessKey
					AND IntBillShipStatusUpdNotification.shipmentID = IntShipShipAllocNotification.shipmentID

				WHERE 
					IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId
			)
	BEGIN
		INSERT INTO MarketplaceActionsProcess 
		(
			sourceFlowName,
			sourceFlowKey,
			sourceFlowKeyDescription,
			targetOperation,
			applicationSystemId,
			creationDate,
			transactionStatus,
			brandId,
			retryCount,
			isDeleted,
			stepCompleted,
			sourceFlowProcessKey
		)
		SELECT DISTINCT
			[sourceFlowName] = 'KiboNotificationInsert',
			[sourceFlowKey] = @notificationId,
			[sourceFlowKeyDescription] = 'shipStatusUpdNotificationId',
			[targetOperation] = Actions.targetOperation,
			[applicationSystemId] = 'OC',
			@date,
			Actions.transactionStatus,
			Brand.brandId,
			0,
			0,
			0,
			@shipmentID
		FROM
			IntBillShipStatusUpdNotification

			INNER JOIN Brand WITH (NOLOCK)
			ON LEFT(IntBillShipStatusUpdNotification.externalOrderID, 2) = Brand.brand

			INNER JOIN MarketplaceOrder 
			ON MarketplaceOrder.isDeleted = 0
			AND MarketplaceOrder.channel = 'OC'
			AND MarketplaceOrder.merchantOrderNumber = IntBillShipStatusUpdNotification.externalOrderID

			CROSS JOIN @Actions Actions
		WHERE
			IntBillShipStatusUpdNotification.IntBillShipStatusUpdNotificationId = @notificationId;

	END;
	---------------

	--TLGESB-764
	IF ((@stateToCode = '500' OR (@stateToCode = '400' AND @isPickup = 'TRANSFER')) AND EXISTS ( --TLGESB-1558
			SELECT
				*
			FROM
				Location WITH (NOLOCK)
			WHERE
				locationId = @locationID AND
				shipmentNotificationPartner = 1
		)
	)
	BEGIN
		DECLARE @shipmentNotificationPartnerHeaderId UNIQUEIDENTIFIER = dbo.NEWID_ORD(NEWID());
		DECLARE @kiboorderHeaderId UNIQUEIDENTIFIER =
			(
				SELECT TOP 1 -- TLGESB-836 ignore duplicates from Kibo
					kiboOrderHeaderId

				FROM
					KiboOrderHeader WITH (NOLOCK)

				WHERE
					[orderID] = @orderID
					AND isDeleted = '0'
					AND flowName = 'getOrder'
			);

		INSERT INTO ShipmentNotificationPartnerHeader
		(
			[shipmentNotificationPartnerHeaderId],
			[orderNumber],
			[shipmentNumber],
			[orderStatusDate],
			[locationId],
			[storeCode],
			[channel],
			[type],
			[cancelled],
			[operator]
		)
		SELECT DISTINCT 
			[shipmentNotificationPartnerHeaderId] = @shipmentNotificationPartnerHeaderId,
			[orderNumber] = @externalOrderID,
			[shipmentNumber] = @shipmentID,
			[orderStatusDate] = @date,
			[locationId] = @locationID,
			[storeCode] = @externalStoreID,
			CASE WHEN channel = 'Farfetch' THEN 'F' ELSE 'T' END,
			[type] = 'S',
			0,
			NULL
		FROM
			KiboOrderHeader WITH (NOLOCK)
		WHERE
			kiboOrderHeaderId = @kiboorderHeaderId;
		
		INSERT INTO ShipmentNotificationPartnerDetail 
		(
			[shipmentNotificationPartnerDetailId],
			[shipmentNotificationPartnerHeaderId],
			[quantity],
			[barcode]
		)
		SELECT
			dbo.NEWID_ORD(NEWID()),
			[shipmentNotificationPartnerHeaderId] = @shipmentNotificationPartnerHeaderId,
			[quantity] = itmQuantity, --COUNT(*),
			[barcode] = RIGHT(RTRIM(itmPartNumber), LEN(itmPartNumber)-2) 
		FROM
			KiboOrderShipmentItems WITH (NOLOCK)

		WHERE
			KiboOrderShipmentItems.shipmentID = @shipmentID
			AND KiboOrderShipmentItems.kiboOrderHeaderId = @kiboorderHeaderId;

		INSERT INTO ShipmentNotificationPartnerProcess
		(
			[shipmentNotificationPartnerProcessId],
			[shipmentNotificationPartnerHeaderId],
			[transactionStatus],
			[transactionDate],
			[retryCount]
		)
		VALUES
		(
			@notificationId,
			@shipmentNotificationPartnerHeaderId,
			@statusToProcess,
			@date,
			0
		)
	END;
	---------------

	--TLGESB-1578
	IF ((@stateToCode = '400' AND @isPickup = 'IN_STORE_PICKUP')) AND EXISTS (
			SELECT
				*
			FROM
				Location WITH (NOLOCK)
			WHERE
				locationId = @locationID AND
				shipmentNotificationPartner = 1
		)
	)
	BEGIN
		DECLARE @shipmentNotificationPartnerHeaderId UNIQUEIDENTIFIER = dbo.NEWID_ORD(NEWID());
		DECLARE @kiboorderHeaderId UNIQUEIDENTIFIER =
			(
				SELECT TOP 1 -- TLGESB-836 ignore duplicates from Kibo
					kiboOrderHeaderId

				FROM
					KiboOrderHeader WITH (NOLOCK)

				WHERE
					[orderID] = @orderID
					AND isDeleted = '0'
					AND flowName = 'getOrder'
			);

		INSERT INTO ShipmentNotificationPartnerHeader
		(
			[shipmentNotificationPartnerHeaderId],
			[orderNumber],
			[shipmentNumber],
			[orderStatusDate],
			[locationId],
			[storeCode],
			[channel],
			[type],
			[cancelled],
			[operator]
		)
		SELECT DISTINCT 
			[shipmentNotificationPartnerHeaderId] = @shipmentNotificationPartnerHeaderId,
			[orderNumber] = @externalOrderID,
			[shipmentNumber] = @shipmentID,
			[orderStatusDate] = @date,
			[locationId] = @locationID,
			[storeCode] = @externalStoreID,
			CASE WHEN channel = 'Farfetch' THEN 'F' ELSE 'T' END,
			[type] = 'S',
			0,
			NULL
		FROM
			KiboOrderHeader WITH (NOLOCK)
		WHERE
			kiboOrderHeaderId = @kiboorderHeaderId;
		
		INSERT INTO ShipmentNotificationPartnerDetail 
		(
			[shipmentNotificationPartnerDetailId],
			[shipmentNotificationPartnerHeaderId],
			[quantity],
			[barcode]
		)
		SELECT
			dbo.NEWID_ORD(NEWID()),
			[shipmentNotificationPartnerHeaderId] = @shipmentNotificationPartnerHeaderId,
			[quantity] = 1, --COUNT(*),
			[barcode] = RIGHT(RTRIM(IntOrdShipStatusOrderDetail.productId), LEN(IntOrdShipStatusOrderDetail.productId)-2) 
		FROM
			KiboOrderShipmentItems WITH (NOLOCK)
			INNER JOIN IntOrdShipStatusOrderHeader WITH (NOLOCK)
			ON KiboOrderShipmentItems.xpoOrderID = IntOrdShipStatusOrderHeader.orderNo
            INNER JOIN IntOrdShipStatusOrderDetail WITH (NOLOCK)
            ON IntOrdShipStatusOrderHeader.intOrdShipStatusOrderHeaderId = IntOrdShipStatusOrderDetail.intOrdShipStatusOrderHeaderId 

		WHERE
			KiboOrderShipmentItems.shipmentID = @shipmentID
			AND IntOrdShipStatusOrderDetail.productStatus = 'ready-for-pickup';

		INSERT INTO ShipmentNotificationPartnerProcess
		(
			[shipmentNotificationPartnerProcessId],
			[shipmentNotificationPartnerHeaderId],
			[transactionStatus],
			[transactionDate],
			[retryCount]
		)
		VALUES
		(
			@notificationId,
			@shipmentNotificationPartnerHeaderId,
			@statusToProcess,
			@date,
			0
		)
	END;
	---------------
	

	SET @jsonResponse = '{"result":"success"}';

END
