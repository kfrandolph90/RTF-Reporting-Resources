label = ""
relabel = ""


# Will become metadataname_label and metadataname_relabel in Looker
metadataname = ''


# Must manually paste from sheets. Make sure there is no space between the open and end quotation marks.
label = '''

'''


relabel = '''


'''

# Replace the end of the line with a comma, and then split on the comma.
label = label.replace("\n",",")
label=label.split(",")

relabel = relabel.replace("\n",",")
relabel=relabel.split(",")


looker_head = ''' view : ''' + metadataname + '''_metadata {

      derived_table: {

        sql:  SELECT *
      FROM (VALUES  '''


sql_meat = ""
for i in range(len(label)):
    sql_meat += ("        (" + "'"+label[i]+"'" + "," + "'"+relabel[i]+"'" + ")")
    if i != len(label)-1:
        sql_meat += "," + "\r\n"

sql_meat += (") t (" + metadataname + "_label, " + metadataname + "_relabel)")
sql_meat += ";;"
sql_meat += "}"


looker_tail1 =  '''dimension: ''' +  metadataname + '''_label {
type: string
sql: ${TABLE}.''' + metadataname + '''_label''' +   ''';;''' + ''' } '''

looker_tail2 = '''dimension: ''' +  metadataname + '''_relabel {
type: string
sql: ${TABLE}.''' + metadataname + '''_relabel''' + ''';;''' + ''' }    } '''

print(looker_head)
print(sql_meat)
print("\r\n")
print(looker_tail1)
print("\r\n")
print(looker_tail2)
