def mergetime(files,of):
    arg = 'mergetime'
    return(['cdo','-b', 'F64',arg,files,of])
