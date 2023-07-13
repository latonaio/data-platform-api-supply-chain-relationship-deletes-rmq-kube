package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-supply-chain-relationship-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-supply-chain-relationship-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"strings"
)

func (c *DPFMAPICaller) GeneralRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.General {

	where := strings.Join([]string{
		fmt.Sprintf("WHERE general.SupplyChainRelationshipID = %d ", input.General.SupplyChainRelationshipID),
		fmt.Sprintf("AND general.Buyer = %d ", input.General.Buyer),
		fmt.Sprintf("AND general.Seller = %d ", input.General.Seller),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	supplyChainRelationship.SupplyChainRelationshipID,
    	supplyChainRelationship.Buyer,
    	supplyChainRelationship.Seller
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_scr_general_data as general 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGeneral(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
