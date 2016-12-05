//var editor; // use a global for the submit and return data rendering in the contact
var otable;
var dataTab;
var textit_prefix = "./api/zambia";
$(document).ready(function() {
	$.fn.dataTable.ext.errMode = 'none';
	
    editor = new $.fn.dataTable.Editor( {
        table: "#flow",
        fields: [ {
                label: "Flow UUID:",
                name: "uuid",
				type: "readonly"
            }, {
                label: "Flow Name:",
                name: "name",
				type: "readonly"
            }, {
                label: "Total Runs:",
                name: "runs",
				type: "readonly"
            }, {
                label: "Completed Runs:",
                name: "completed_runs",
				type: "readonly"
            }, {
                label: "Created Date:",
                name: "created_on",
				type: "readonly"
            }, {
                label: "Country:",
                name: "country",
				type: "readonly"
            }, {
                label: "Season:",
                name: "season",
				fieldInfo: "Ex: Planting, Growing, Harvesting, Inter-Season"
            }, {
                label: "Creator:",
                name: "creator",
				fieldInfo: "Ex: Jacob"
            }, {
                label: "Flow Type:",
                name: "flow_type",
				fieldInfo: "Ex: Test, Pilot, Regular, Unused"
            }, {
                label: 'Run Start Date:',
				name: 'run_start_date',
                type: 'datetime',
                format: 'YYYY-MM-DD HH:mm:ss',
                fieldInfo: 'EX: 03-10-2016 2.30 PM'
            }, {
                label: 'Run End Date:',
                name: 'run_end_date',
                type: 'datetime',
                format: "YYYY-MM-DD HH:mm:ss",
                fieldInfo: 'EX: 09-10-2016 11.55 PM'
            }
        ]
    } );
	
	
	$('#flow').on( 'click', 'tbody td:not(:first-child)', function (e) {
        editor.inline( this, {
            buttons: { label: '&gt;', fn: function () { 
			
			var table = $('#flow').DataTable();
			
			$('div .DTE_Form_Buttons').click(function () {			
				
				var season = $("#DTE_Field_season").val();
				var creator = $("#DTE_Field_creator").val();
				var flow_type = $("#DTE_Field_flow_type").val();
				var run_start_date_full = $("#DTE_Field_run_start_date").val();				
				var run_end_date_full = $("#DTE_Field_run_end_date").val();				
				var uuid = $(this).closest('tr').find('td:eq(1)').text();
				
				if(season==undefined){
					season = $(this).closest('tr').find('td:eq(4)').text();
				}if(creator==undefined){
					creator = $(this).closest('tr').find('td:eq(5)').text();
				}if(flow_type==undefined){
					flow_type = $(this).closest('tr').find('td:eq(6)').text();
				}if(run_start_date_full==undefined){
					run_start_date_full = $(this).closest('tr').find('td:eq(7)').text();
					
					if(run_start_date_full==""){
					run_start_date = "";
					run_start_time = "";
					}else{
					run_start_date_sa = new Date(run_start_date_full).toISOString();
					run_start_date = run_start_date_sa.split("T")[0];
					run_start_time = run_start_date_sa.split("T")[1];
					}
					
				}else{
					run_start_date_sa = new Date(run_start_date_full).toISOString();
					run_start_date = run_start_date_sa.split("T")[0];
					run_start_time = run_start_date_sa.split("T")[1];
				
				}if(run_end_date_full==undefined){
					run_end_date_full = $(this).closest('tr').find('td:eq(8)').text();
					
					if(run_end_date_full==""){
					run_end_date = "";
					run_end_time = "";
					}else{
						run_end_date_sa = new Date(run_end_date_full).toISOString();
						run_end_date = run_end_date_sa.split("T")[0];
						run_end_time = run_end_date_sa.split("T")[1];
					}
					
				}else{
					run_end_date_sa = new Date(run_end_date_full).toISOString();
					run_end_date = run_end_date_sa.split("T")[0];
					run_end_time = run_end_date_sa.split("T")[1];
				}
				
				var dataflowArray = {};
				dataflowArray['uuid'] = uuid;
				
				if (season != ""){
					dataflowArray['season'] = season;
				}if (creator != ""){
					dataflowArray['creator'] = creator;
				}if (flow_type != ""){
					dataflowArray['flow_type'] = flow_type;
				}if (run_start_date != ""){
					dataflowArray['run_start_date'] = run_start_date;
				}if (run_start_time != ""){
					dataflowArray['run_start_time'] = run_start_time;
				}if (run_end_date != ""){
					dataflowArray['run_end_date'] = run_end_date;
				}if (run_end_time != ""){
					dataflowArray['run_end_time'] = run_end_time;
				}
				
				
				
				$.ajax({
					type: "POST",
					//in this method insert the data in your database
					url: textit_prefix + "/flows",
					contentType: "application/json; charset=utf-8",
					data: JSON.stringify(dataflowArray),
					
					dataType: "json",
					success: AjaxUpdateDataSucceeded,
					error: AjaxUpdateDataFailed
				});
			});
			
			
			
			} }
        } );
    } );
 
    $('#flow').DataTable( {
        dom: "Bfrtip",
        ajax: {
			url:  textit_prefix + "/flows",
            dataSrc : ""
		},
        columns: [
			{
                data: null,
                defaultContent: '',
                className: 'select-checkbox',
                orderable: false
            },
			{ data: "uuid"},
			{ data: "name" },
			{ data: "runs", "visible": false},
			{ data: "completed_runs", "visible": false},
			{ data: "created_on" },
			{ data: "country", "visible": false},
			{ data: "season" },
			{ data: "creator" },
			{ data: "flow_type" },
			{ data: "run_start_date"},
			{ data: "run_end_date"}
        ],
		
        select: {
            style:    'os',
            selector: 'td:first-child'
        },
        buttons: [
            { extend: "edit", editor: editor }
        ]
    } );

} );

function AjaxUpdateDataSucceeded(result) {
    if (result != "[]") {
		$('#success').html(""); //Clear error span
    	$('#success').append( "<span class='alert alert-success'><strong>Success! </strong>" + ' ' +" Metadata field successfully updated" + "<br></span>");
		setTimeout(function(){
		   window.location.reload();
		}, 2000);
		
    }
}

function AjaxUpdateDataFailed(result) {
	//alert(result.responseText);
    //alert(result.status + ' ' + result.statusText);
	$('#failure').html(""); //Clear error span
    $('#failure').append("<span class='alert alert-danger'><strong>Failure! </strong>" + ' ' + result.responseText + "<br></span>");
	setTimeout(function(){
		   window.location.reload();
	}, 2000);
}






