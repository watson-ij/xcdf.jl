using CodecZlib
using TranscodingStreams
using SimpleChecksums

XCDF_VERSION = 3
XCDF_DATUM_WIDTH_BYTES = 8
XCDF_DATUM_WIDTH_BITS  = 64

# enum XCDFFrameType
XCDF_NONE           = 0xC93F2B8A
XCDF_FILE_HEADER    = 0x436FC8A4
XCDF_BLOCK_HEADER   = 0x160E17E4
XCDF_BLOCK_DATA     = 0x37DF239D
XCDF_FILE_TRAILER   = 0xBD340AF6
XCDF_DEFLATED_FRAME = 0x7E4A26B7

# enum XCDFFieldType {
XCDF_UNSIGNED_INTEGER    = 0
XCDF_SIGNED_INTEGER      = 1
XCDF_FLOATING_POINT      = 2

NO_PARENT = ""

struct XcdfFrame
    type::UInt32
    size::UInt32
    checksum::UInt32
    buffer::Array{UInt8}
    deflated::Bool
    startOffset::Int64
end

function readXcdfFrame(file::IOStream)
    startOffset = position(file)
    type = read(file, UInt32)
    size = read(file, UInt32)
    checksum = read(file, UInt32)
    deflated = false
    if type == XCDF_DEFLATED_FRAME
        type = read(file, UInt32)
        deflated = true
    end
    buffer = Array{UInt8}(undef, size)
    read!(file, buffer)
    # todo checksum calculation and check
    check = adler32(buffer)
    if check != checksum
        println("Bad read")
    end
    return XcdfFrame(type, size, checksum, buffer, deflated, startOffset)
end

function xcdfArray(frame::XcdfFrame)
    if frame.deflated
        return transcode(ZlibDecompressor, frame.buffer)
    else
        return frame.buffer
    end
end

function xcdfBuffer(frame::XcdfFrame)
    if frame.deflated
        buffer = ZlibDecompressorStream(IOBuffer(frame.buffer))
    else
        buffer = IOBuffer(frame.buffer)
    end
    return buffer
end

struct XcdfFieldDescriptor
    name::String
    type::Int8
    rawResolution::UInt64
    parentName::String
end

struct XcdfFieldAlias
    name::String
    expression::String
    type::Int8
end

struct XcdfFileHeader
    fileTrailerPtr::UInt64
    version::UInt32
    fieldDescriptors::Array{XcdfFieldDescriptor}
    fieldAliases::Array{XcdfFieldAlias}
end

function readXcdfString(buffer)
    size = read(buffer, UInt32)
    if size == 0
        return ""
    end
    array = Array{UInt8}(undef, size-1)
    read!(buffer, array)
    null = read(buffer, UInt8)
    if null != 0
        println("Bad string")
    end
    return String(array)
end

function unpackFileHeader(frame::XcdfFrame)
    if frame.type != XCDF_FILE_HEADER
        println("Bad header")
        println(frame)
    end
    buffer = xcdfBuffer(frame)
    version = read(buffer, UInt32)
    if version > XCDF_VERSION
        println("Can't read version ", version)
    end
    fileTrailerPtr = read(buffer, UInt64)
    nFields = read(buffer, UInt32)
    fieldDescriptors = Array{XcdfFieldDescriptor}(undef, nFields)
    for i in 1:nFields
        name = readXcdfString(buffer)
        type = read(buffer, UInt8)
        rawResolution = read(buffer, UInt64)
        parentName = readXcdfString(buffer)
        fieldDescriptors[i] = XcdfFieldDescriptor(name, type, rawResolution, parentName)
    end
    fieldAliases = Array{XcdfFieldAlias}(undef, 0)
    if version > 2
        nAliases = read(buffer, UInt32)
        fieldAliases = Array{XcdfFieldAlias}(undef, nAliases)
        for i in 1:nAliases
            name = readXcdfString(buffer)
            expression = readXcdfString(buffer)
            type = read(buffer, UInt8)
            fieldAliases[i] = XcdfFieldDescriptor(name, expression, type)
        end
    end
    return XcdfFileHeader(fileTrailerPtr, version, fieldDescriptors, fieldAliases)
end

struct XcdfBlockEntry
    nextEventNumber::UInt64
    filePtr::UInt64
end

struct XcdfFileTrailer
    eventCount::UInt64
    entries::Array{XcdfBlockEntry}
    comments::Array{String}
end


function unpackFileTrailer(frame::XcdfFrame, version::UInt32)
    if frame.type != XCDF_FILE_TRAILER
        println("Bad trailer")
        println(frame)
    end
    buffer = xcdfBuffer(frame)
    eventCount = read(buffer, UInt64)
    nEntries = read(buffer, UInt32)
    entries = Array{XcdfBlockEntry}(undef, nEntries)
    for i in 1:nEntries
        entries[i] = XcdfBlockEntry(read(buffer, UInt64), read(buffer, UInt64))
    end
    nComments = read(buffer, UInt32)
    comments = Array{String}(undef, nComments)
    for i in 1:nComments
        comments[i] = readXcdfString(buffer)
    end
    if version > 2
        # globals
        # alias descriptors
    end
    return XcdfFileTrailer(eventCount, entries, comments)
end

mutable struct XcdfBlockData
    data::Vector{UInt8}
    index::UInt
    indexBits::UInt
end

mutable struct XcdfField{T}
    parent::Ptr{XcdfField}
    resolution::T
    activeSize::UInt32
    activeMin::T
    activeMax::T
    value::T
end

mutable struct Xcdf
    file::IOStream
    header::XcdfFileHeader
    trailer::XcdfFileTrailer
    fields::Array{XcdfField}
    endHeaderPtr::UInt64
    blockData::XcdfBlockData
    blockEventCount::UInt32
    blockCount::UInt64
    eventCount::UInt64
end

function openXcdf(filename::String)
    file = open(filename)
    fheader = readXcdfFrame(file)
    header = unpackFileHeader(fheader)
    endHeaderPtr = position(file)
    seek(file, fheader.startOffset + header.fileTrailerPtr)
    trailer = readXcdfFrame(file)
    trailer = unpackFileTrailer(trailer, header.version)
    seek(file, endHeaderPtr)
    nFields = size(header.fieldDescriptors)[1]
    fields = Array{XcdfField}(undef, nFields)
    for i in 1:nFields
        T = UInt64
        if header.fieldDescriptors[i].type == XCDF_SIGNED_INTEGER T = Int64 end
        if header.fieldDescriptors[i].type == XCDF_FLOATING_POINT T = Float64 end
        fields[i] = XcdfField{T}(Ptr{XcdfField}(), reinterpret(T, header.fieldDescriptors[i].rawResolution), 0, 0, 0, 0)
    end
    return Xcdf(file, header, trailer,fields, endHeaderPtr, XcdfBlockData([], 1, 0), 0, 0, 0)
end

struct XcdfFieldHeader
    rawActiveMin::UInt64
    activeSize::UInt8
end

struct XcdfBlockHeader
    eventCount::UInt32
    headers::Array{XcdfFieldHeader}
end

function unpackBlockHeader(frame::XcdfFrame)
    buffer = xcdfBuffer(frame)
    eventCount = read(buffer, UInt32)
    nHeaders = read(buffer, UInt32)
    headers = Array{XcdfFieldHeader}(undef, nHeaders)
    for i in 1:nHeaders
        headers[i] = XcdfFieldHeader(read(buffer, UInt64), read(buffer, UInt8))
    end
    return XcdfBlockHeader(eventCount, headers)
end

function calculateValue(field::XcdfField, datum::UInt64)
    field.value = datum * field.resolution + field.activeMin
end

function calculateValue(field::XcdfField{UInt64}, datum::UInt64)
    field.value = datum * field.resolution + field.activeMin
end

function calculateValue(field::XcdfField{Int64}, datum::UInt64)
    field.value = Int64(datum) * field.resolution + field.activeMin
end

function calculateValue(field::XcdfField{Float64}, datum::UInt64)
    field.value = Float64(datum) * field.resolution + field.activeMin
end

function getDatum(data::XcdfBlockData, size::UInt32)::UInt64
    # println("size: ", size, " index: ", data.index)
    if size == 0
        return UInt64(0x0)
    end
    datum = reinterpret(UInt64, data.data[data.index:data.index+7])[1] >> data.indexBits
    tot = size + data.indexBits
    if tot > 64 # data spread across 9 bytes
        datum |= UInt64(data.data[data.index+8]) << (64 - data.indexBits)
    end
    if size < 64
        mask = UInt64((UInt64(1) << size) - 1)
        datum &= mask
    end
    data.index += tot >> 3
    data.indexBits = tot & 0x07
    return datum
end

Base.eltype(::Type{XcdfField{T}}) where T = T
function xcdfReadEvent(x::Xcdf)
    if x.blockEventCount == 0
        if !xcdfNextBlockWithEvents(x)
            return false
        end
    end
    for field in x.fields
        datum = getDatum(x.blockData, field.activeSize) 
        calculateValue(field, datum)
    end
    x.blockEventCount -= 1
    x.eventCount += 1
    return true
end


function unpackBlockData(frame::XcdfFrame)
    array = xcdfArray(frame)
    for i in 1:8 # make sure we have enough space at the end to unpack the bits into uint64
        push!(array, 0)
    end
    return XcdfBlockData(array, 1, 0)
end

function xcdfReadBlock(x::Xcdf)
    frame = readXcdfFrame(x.file)
    if frame.type == XCDF_FILE_TRAILER
        return false
    end
    if frame.type != XCDF_BLOCK_HEADER
        println("Corrupt xcdf file")
        return false
    end
    header = unpackBlockHeader(frame)
    if size(header.headers) != size(x.fields)
        println("Corrupt xcdf file: wrong number of block headers")
        return false
    end
    for (i, header) in enumerate(header.headers)
        x.fields[i].activeMin = reinterpret(typeof(x.fields[i].value), header.rawActiveMin)
        x.fields[i].activeSize = header.activeSize
    end
    x.eventCount += x.blockEventCount # add remaining events from prev. block
    x.blockEventCount = header.eventCount
    frame = readXcdfFrame(x.file)
    if frame.type != XCDF_BLOCK_DATA
        println("Corrupt xcdf file: block data does not follow block header")
    end
    x.blockData = unpackBlockData(frame)
    x.blockCount += 1
    return true
end

function xcdfNextBlockWithEvents(x::Xcdf)
    while true
        if !xcdfReadBlock(x)
            return false
        end
        if x.blockEventCount > 0
            return true
        end
    end
end

function Base.iterate(x::Xcdf)
    xcdfReadEvent(x)
    return (true, 0)
end

function Base.iterate(x::Xcdf, i::Int)
    if !xcdfReadEvent(x)
        return nothing
    end
    return (true, 0)
end

function show(io::IO, x::Xcdf)
    println(io, "Xcdf::$(x.file.name) ; version:$(Int.(x.header.version))")
    println(" Fields: ")
    for (i, field) in enumerate(x.header.fieldDescriptors)
        println("  ",field.name, " ", x.fields[i].resolution, " ",  typeof(x.fields[i].value))
    end
    println()
    print(" Aliases: ")
    for field in x.header.fieldAliases
        print(field.name, " ")
    end
    println()
    println(" nEvents: $(x.trailer.eventCount)")
    println(" Comments::")
    for (idx, comment) in enumerate(x.trailer.comments)
        println("  ", comment)
        if idx > 3
            println("  ...")
            break
        end
    end
end

Base.show(io::IO, x::Xcdf) = show(io, x)

using Profile
function main()
    fname = "/store/user/iawatson/hawc/crab-strip/4l4l/reco/combined/run008746.xcd"
    x = openXcdf(fname)
    # print(x)
    ii = 0
    #Profile.@profile
    (for (idx, event) in enumerate(x)
        ii += 1
        # if ii > 0
        #     break
        # end
    end)
    println(ii, " events")
    #open(Profile.print, "prot.txt", "w")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
