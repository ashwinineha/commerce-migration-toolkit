delimiter //
CREATE PROCEDURE fillProducts(amount int)
BEGIN
    DECLARE i int DEFAULT 1;
    WHILE i <= amount DO
        INSERT INTO cmtsource.products 
				(`hjmpTS`,
				`createdTS`,
				`modifiedTS`,
				`TypePkString`,
				`OwnerPkString`,
				`PK`,
				`sealed`,
				`p_code`,
				`p_unit`,
				`p_thumbnail`,
				`p_picture`,
				`p_catalog`,
				`p_catalogversion`,
				`p_onlinedate`,
				`p_offlinedate`,
				`p_ean`,
				`p_supplieralternativeaid`,
				`p_buyerids`,
				`p_manufactureraid`,
				`p_manufacturername`,
				`p_erpgroupbuyer`,
				`p_erpgroupsupplier`,
				`p_deliverytime`,
				`p_specialtreatmentclasses`,
				`p_order`,
				`p_approvalstatus`,
				`p_contentunit`,
				`p_numbercontentunits`,
				`p_minorderquantity`,
				`p_maxorderquantity`,
				`p_orderquantityinterval`,
				`p_pricequantity`,
				`p_normal`,
				`p_thumbnails`,
				`p_detail`,
				`p_logo`,
				`p_data_sheet`,
				`p_others`,
				`p_startlinenumber`,
				`p_endlinenumber`,
				`p_varianttype`,
				`p_europe1pricefactory_ppg`,
				`p_europe1pricefactory_ptg`,
				`p_europe1pricefactory_pdg`,
				`p_productorderlimit`,
				`p_galleryimages`,
				`p_reviewcount`,
				`p_reviewrating`,
				`p_soldindividually`,
				`p_sequenceid`,
				`aCLTS`,
				`propTS`,
				`p_baseproduct`,
				`p_swatchcolors`,
				`p_genders`)
        SELECT 	`hjmpTS`,
				`createdTS`,
				`modifiedTS`,
				`TypePkString`,
				`OwnerPkString`,
				i,
				`sealed`,
				CONCAT('MASSPRODUCT',i),
				`p_unit`,
				`p_thumbnail`,
				`p_picture`,
				`p_catalog`,
				`p_catalogversion`,
				`p_onlinedate`,
				`p_offlinedate`,
				`p_ean`,
				`p_supplieralternativeaid`,
				`p_buyerids`,
				`p_manufactureraid`,
				`p_manufacturername`,
				`p_erpgroupbuyer`,
				`p_erpgroupsupplier`,
				`p_deliverytime`,
				`p_specialtreatmentclasses`,
				`p_order`,
				`p_approvalstatus`,
				`p_contentunit`,
				`p_numbercontentunits`,
				`p_minorderquantity`,
				`p_maxorderquantity`,
				`p_orderquantityinterval`,
				`p_pricequantity`,
				`p_normal`,
				`p_thumbnails`,
				`p_detail`,
				`p_logo`,
				`p_data_sheet`,
				`p_others`,
				`p_startlinenumber`,
				`p_endlinenumber`,
				`p_varianttype`,
				`p_europe1pricefactory_ppg`,
				`p_europe1pricefactory_ptg`,
				`p_europe1pricefactory_pdg`,
				`p_productorderlimit`,
				`p_galleryimages`,
				`p_reviewcount`,
				`p_reviewrating`,
				`p_soldindividually`,
				`p_sequenceid`,
				`aCLTS`,
				`propTS`,
				`p_baseproduct`,
				`p_swatchcolors`,
				`p_genders`
        FROM cmtsource.products WHERE PK=8796110618625;
        SET i = i + 1;
    END WHILE;
END//
delimiter ;