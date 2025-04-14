Sub SplitEachWorksheetToCSV()
    Dim FPath As String
    Dim ws As Worksheet
    
    ' Get the folder path of the active workbook
    FPath = Application.ActiveWorkbook.Path
    
    ' Disable screen updating and alerts for better performance
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False
    
    ' Loop through each worksheet in the workbook
    For Each ws In ThisWorkbook.Sheets
        ' Copy the worksheet to a new workbook
        ws.Copy
        
        ' Save the new workbook as a CSV file
        Application.ActiveWorkbook.SaveAs _
            Filename:=FPath & "\" & ws.Name & ".csv", _
            FileFormat:=xlCSV, _
            CreateBackup:=False
        
        ' Close the new workbook without saving changes
        Application.ActiveWorkbook.Close SaveChanges:=False
    Next ws
    
    ' Re-enable screen updating and alerts
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    
    MsgBox "All worksheets have been exported as CSV files.", vbInformation
End Sub

