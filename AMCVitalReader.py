import io
import gzip
import struct
import csv
import datetime
import numpy as np

class KST(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=9) + self.dst(dt)
    def dst(self, dt):
        return datetime.timedelta(0)
    def tzname(self, dt):
        return "KST"

class vital_device:
    did = ""
    typename = ""
    devname = ""
    port = ""

class vital_track:
    tid = ""
    rec_type = ""
    rec_fmt = ""
    name = ""
    unit = ""
    minval = ""
    maxval = ""
    color = ""
    srate = 0
    adc_gain = ""
    adc_offset = ""
    mon_type = ""
    did = ""
    st = 0
    dt = []
    v_number = []
    v_wave = []
    v_string = []

class vital_record:
    infolen = 0
    dt = 0
    tid = 0
    data = []

def convert_binary_to_string(binary_list):
    r = []
    for i in range(len(binary_list)):
        r.append(binary_list[i].decode('utf-8'))
    return r

class vital_reader(object):

    def __init__(self,file):
        self.filename = file
        self.vital_file = gzip.open(file,"rb")
        self.device = {}
        self.track = {}
        self.record = []

    def get_gzip_size(self):
        with open(self.filename, 'rb') as f:
            f.seek(-4, 2)
            data = f.read(4)
        size = struct.unpack('<L', data)[0]
        return size

    def read_header(self):
        with gzip.open(self.filename,"rb") as vital_file:
            vital_file.seek(0)
            self.signature = vital_file.read(4)
            self.version = int.from_bytes(vital_file.read(4), byteorder="little", signed=False)
            self.headerlen = int.from_bytes(vital_file.read(2), byteorder="little", signed=False)
            self.tzbias = int.from_bytes(vital_file.read(2), byteorder="little", signed=False)
            self.inst_id = int.from_bytes(vital_file.read(4), byteorder="little", signed=False)
            self.prog_ver = int.from_bytes(vital_file.read(4), byteorder="little", signed=False)

    def write_track_info(self, filename):
        fieldnames = ['did', 'tid', 'rec_type', 'rec_format', 'name', 'unit', 'minval', 'maxval', 'color', 'srate', 'adc_gain', 'adc_offset', 'mon_type', 'dt_length', 'vn_length']
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting = csv.QUOTE_MINIMAL)
            csv_writer.writerow(fieldnames)
            for itrack in self.track:
                csv_writer.writerow([self.track[itrack].did, self.track[itrack].tid, self.track[itrack].rec_type, self.track[itrack].rec_fmt,
                                    self.track[itrack].name, self.track[itrack].unit, self.track[itrack].minval, self.track[itrack].maxval,
                                    self.track[itrack].color, self.track[itrack].srate, self.track[itrack].adc_gain, self.track[itrack].adc_offset,
                                    self.track[itrack].mon_type, len(self.track[itrack].dt), len(self.track[itrack].v_number)])

    def write_device_info(self, filename):
        fieldnames = ['did', 'typename', 'devname', 'port']
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting = csv.QUOTE_MINIMAL)
            csv_writer.writerow(fieldnames)
            for idevice in self.device:
                csv_writer.writerow([self.device[idevice].did, self.device[idevice].typename,
                                     self.device[idevice].devname, self.device[idevice].port])
    def analyze_dt(self, filename):
        fieldnames = []
        firstrow = {}
        max_len_dt = 0
        for itrack in self.track:
            fieldnames.append(itrack)
            firstrow[itrack] = self.track[itrack].dt[0]
            if (max_len_dt < len(self.track[itrack].dt)):
                max_len_dt = len(self.track[itrack].dt)
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting = csv.QUOTE_MINIMAL)
            csv_writer.writerow(fieldnames)
            for i in range(max_len_dt):
                row = []
                for itrack in self.track:
                    if ( i < len(self.track[itrack].dt) ):
                        row.append(self.track[itrack].dt[i] - firstrow[itrack])
                    else:
                        row.append(0)
                csv_writer.writerow(row)

    def analyze_length(self, filename):
        fieldnames = []
        firstrow = {}
        max_len_dt = 0
        for itrack in self.track:
            fieldnames.append(itrack)
            firstrow[itrack] = self.track[itrack].dt[0]
            if (max_len_dt < len(self.track[itrack].dt)):
                max_len_dt = len(self.track[itrack].dt)
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting = csv.QUOTE_MINIMAL)
            csv_writer.writerow(fieldnames)
            for i in range(max_len_dt):
                row = []
                for itrack in self.track:
                    if ( i < len(self.track[itrack].dt) ):
                        row.append(self.track[itrack].v_number[i])
                    else:
                        row.append(0)
                csv_writer.writerow(row)

    def read_value_csv_form(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                for i in range(len(self.track[itrack].dt)):
                    r.append([self.track[itrack].dt[i], self.track[itrack].v_number[i]])
                return r
        raise ValueError('No such a track exists.')

    def read_wave_csv_form(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                last_p = 0
                for vi in range(len(self.track[itrack].v_number)):
                    for i in range(self.track[itrack].v_number[vi]):
                        if i == 0:
                            if last_p == 0:
                                r.append([self.track[itrack].dt[i], self.track[itrack].v_wave[last_p + i]])
                            elif abs(self.track[itrack].dt[i] - self.track[itrack].dt[i-1] + self.track[itrack].v_number[i-1] / self.track[itrack].srate) < 1.0:
                                r.append([self.track[itrack].dt[i], self.track[itrack].v_wave[last_p + i]])
                            else:
                                r.append([0, self.track[itrack].v_wave[last_p + i]])
                        else:
                            r.append([0, self.track[itrack].v_wave[last_p + i]])
                    last_p = last_p + self.track[itrack].v_number[vi]
                return r
        raise ValueError('No such a track exists.')

    # Assumes that there's no duplicated track name. Needs to be changed.

    def read_wave_datetime_interval(self, typename, trackname, datetime_start, datetime_end):
        dt, wave_val = self.read_wave_datetime(typename, trackname)
        p_start = 0
        while dt[p_start] < datetime_start if p_start < len(dt) else False:
            p_start += 1
        p_end = p_start
        while dt[p_end] < datetime_end if p_end < len(dt) else False:
            p_end += 1

        if p_start < p_end:
            return dt[p_start:p_end], wave_val[p_start:p_end]
        return [], []

    def read_wave_datetime(self, typename, trackname):
        wave, srate = self.read_wave(typename, trackname)
        dt = []
        val = []

        for i in range(len(wave)):
            for j in range(len(wave[i][1])):
                dt.append(datetime.datetime.fromtimestamp(wave[i][0]+j/srate))
                val.append(wave[i][1][j])

        return dt, val

    def read_wave(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                t_track = self.track[itrack]
                start_p = 0
                last_p = t_track.v_number[0]
                start_dt = t_track.dt[0]
                for vi in range(1, len(t_track.v_number)):
                    if abs(t_track.dt[vi] - t_track.dt[vi-1] - t_track.v_number[vi-1] / t_track.srate) > 1.0:
                        print ("new segment was found.", vi, t_track.dt[vi], t_track.dt[vi-1], t_track.v_number[vi-1], t_track.srate)
                        r.append([start_dt, self.track[itrack].v_wave[start_p:last_p]]) # add a vector
                        start_dt = self.track[itrack].dt[vi]
                        start_p = last_p
                    last_p = last_p + self.track[itrack].v_number[vi]
                r.append([start_dt, self.track[itrack].v_wave[start_p:last_p]])  # add a vector
                return r, self.track[itrack].srate
        raise ValueError('No such a track exists.')

    def read_number_datetime_interval(self, typename, trackname, datetime_start, datetime_end):
        dt, number = self.read_number_datetime(typename, trackname)
        p_start = 0
        while dt[p_start] < datetime_start if p_start < len(dt) else False:
            p_start += 1
        p_end = p_start
        while dt[p_end] < datetime_end if p_end < len(dt) else False:
            p_end += 1

        if p_start < p_end:
            return dt[p_start:p_end], number[p_start:p_end]
        return [],[]

    def read_number_datetime(self, typename, trackname):
        dt, number = self.read_number_utc(typename, trackname)
        new_dt = []
        for i in range(len(dt)):
            new_dt.append(datetime.datetime.fromtimestamp(dt[i]))
        return new_dt, number

    def read_number_utc(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                return self.track[itrack].dt, self.track[itrack].v_number
        raise ValueError('No such a track exists.')

    def read_string_decoded(self, typename, trackname):
        dt, v_string = self.read_string(typename, trackname)
        v_string_decoed = convert_binary_to_string(v_string)
        return dt, v_string_decoed

    def read_string(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                return self.track[itrack].dt, self.track[itrack].v_string
        raise ValueError('No such a track exists.')

    def read_track(self, typename, trackname):
        did = 0
        r = []
        if typename != '':
            for idevice in self.device:
                if self.device[idevice].typename.decode('utf-8') == typename:
                    did = self.device[idevice].did
            if did==0:
                raise ValueError('No such a device exists.')
        for itrack in self.track:
            if self.track[itrack].name.decode('utf-8') == trackname and self.track[itrack].did == did:
                return self.track[itrack]
        raise ValueError('No such a track exists.')

    def read_packets(self):
        i = 0
        try:
            with gzip.open(self.filename, "rb") as vital_file:
                while True:
                    if (i == 0):
                        vital_file.read(10 + self.headerlen)
#                    print("Iteration : ", i)
                    type = int.from_bytes(vital_file.read(1), byteorder="little", signed=False)
                    datalen = int.from_bytes(vital_file.read(4), byteorder="little", signed=False)
#                    print("Type : ", type, "\tData Length : ", datalen, "\tFile Pointer : ", vital_file.tell())
                    if ( datalen == 0 ):
#                        print ("Packet with 0 length was found. Ignoring.")
                        break
                    else:
                        packet_data = io.BytesIO(vital_file.read(datalen))
                        if (type == 0):  # SAVE_TRKINFO
                            tid = int.from_bytes(packet_data.read(2), byteorder="little", signed=False)
                            self.track[tid] = vital_track()
                            self.track[tid].tid = tid
                            self.track[tid].rec_type = int.from_bytes(packet_data.read(1), byteorder="little", signed=False)
                            self.track[tid].rec_fmt = int.from_bytes(packet_data.read(1), byteorder="little", signed=False)
                            self.track[tid].name = packet_data.read(
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                            self.track[tid].unit = packet_data.read(
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                            self.track[tid].minval = struct.unpack('<f', packet_data.read(4))[0]
                            self.track[tid].maxval = struct.unpack('<f', packet_data.read(4))[0]
                            self.track[tid].color = packet_data.read(4)
                            self.track[tid].srate = struct.unpack('<f', packet_data.read(4))[0]
                            self.track[tid].adc_gain = struct.unpack('<d', packet_data.read(8))[0]
                            self.track[tid].adc_offset = struct.unpack('<d', packet_data.read(8))[0]
                            self.track[tid].mon_type = int.from_bytes(packet_data.read(1), byteorder="little", signed=False)
                            self.track[tid].did = int.from_bytes(packet_data.read(4), byteorder="little", signed=False)
                            self.track[tid].dt = []
                            self.track[tid].v_number = []
                            self.track[tid].v_wave = []
                            self.track[tid].v_string = []
#                            print("Track append complete", tid)
                        elif (type == 1):  # SAVE_REC
#                            print("Save Record")
                            rec = vital_record()
                            rec.infolen = int.from_bytes(packet_data.read(2), byteorder="little", signed=False)
                            rec.dt = struct.unpack('<d', packet_data.read(8))[0]
                            rec.tid = int.from_bytes(packet_data.read(2), byteorder="little", signed=False)
                            if (self.track[rec.tid].did == ""):
                                print("Undefined track id was found in a record packet.")
                                exit(1)
                            if (len(self.track[rec.tid].dt) != len(self.track[rec.tid].v_number)):
                                print("Vector size is wrong. Track : ", rec.tid)
                                exit(1)
                            if (self.track[rec.tid].rec_type == 1 or self.track[rec.tid].rec_type == 6):  # Wave
#                                print("wave")
                                num = int.from_bytes(packet_data.read(4), byteorder="little", signed=False)
                                self.track[rec.tid].dt.append(rec.dt)
                                self.track[rec.tid].v_number.append(num)
                                if (self.track[rec.tid].st == 0):
                                    self.track[rec.tid].st = rec.dt
#                                else:
#                                    if len(self.track[rec.tid].dt) >= 2:
#                                        if (abs((rec.dt - self.track[rec.tid].dt[-2]) - num / self.track[rec.tid].srate) >= 3):
#                                            print("Time gap is more than 3 second in the wave type track.")
                                rec.data = []

                                if (self.track[rec.tid].rec_fmt == 1):
                                    listval = list(struct.unpack('<'+'f'*num,packet_data.read(4*num)))
                                    rec.data.extend(listval)
                                    self.track[rec.tid].v_wave.extend(listval)
                                elif (self.track[rec.tid].rec_fmt == 5 or self.track[rec.tid].rec_fmt == 6):
                                    self.track[rec.tid].v_string.append(packet_data.read(2*num))
                                    listval = list(struct.unpack('<'+'h'*num,self.track[rec.tid].v_string[-1]))
                                    for p in range(len(listval)):
                                        self.track[rec.tid].v_wave.append(listval[p] * self.track[rec.tid].adc_gain + self.track[rec.tid].adc_offset )

                            elif (self.track[rec.tid].rec_type == 2):  # Number
#                                print("number")
#                                print(self.track[rec.tid].rec_fmt)
                                if (self.track[rec.tid].rec_fmt == 1):  # FMT_FLOAT
#                                    print("FMT_FLOAT")
                                    value = struct.unpack('<f', packet_data.read(4))[0]
                                    self.track[rec.tid].dt.append(rec.dt)
                                    self.track[rec.tid].v_number.append(value)
#                                    print(value)
                                    rec.data.append(value)
                                else:
                                    print("Unknown Format, add codes")
                                    exit(1)
                            elif (self.track[rec.tid].rec_type == 5):  # String
                                self.track[rec.tid].dt.append(rec.dt)
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False)
                                sval = packet_data.read(int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                                self.track[rec.tid].v_number.append(len(sval))
                                self.track[rec.tid].v_string.append(sval)
                            else:
                                print("Unknown Record Type")
                                exit(1)
                            self.record.append(rec)

                        elif (type == 6):  # SAVE_CMD
                            cmd = int.from_bytes(packet_data.read(1), byteorder="little", signed=False)
                            if (cmd == 5):  # CMD_ORDER
                                cnt = int.from_bytes(packet_data.read(2), byteorder="little", signed=False)
                                tv = []
#                                print("CMD ORDER")
                                for i in range(cnt):
                                    tv.append(int.from_bytes(packet_data.read(2), byteorder="little", signed=False))
#                                print(tv)
                            elif (cmd == 6):  # CMD_RESET_EVENTS
                                print("Reset Events : code required")
                                # Do nothing
                            else:
                                print("Error. Unknown Command")
                                print(cmd)
                                exit(1)
                        elif (type == 9):  # SAVE_DEVINFO
                            device = vital_device()
                            device.did = int.from_bytes(packet_data.read(4), byteorder="little", signed=False)
                            device.typename = packet_data.read(
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                            device.devname = packet_data.read(
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                            device.port = packet_data.read(
                                int.from_bytes(packet_data.read(4), byteorder="little", signed=False))
                            self.device[device.did] = device
#                            print("Devinfo append complete")
                    i += 1

        except EOFError as e:
            print("Ignoring EOF Error.")
            print(e)

        for itrack in self.track:
            if self.track[itrack].rec_type == 1 or self.track[itrack].rec_type == 2:
                self.track[itrack].dt = np.array(self.track[itrack].dt, dtype=np.float64)
                self.track[itrack].v_wave = np.array(self.track[itrack].v_wave, dtype=np.float32)
                if self.track[itrack].rec_type == 1:
                    self.track[itrack].v_number = np.array(self.track[itrack].v_number, dtype=np.int32)
                elif self.track[itrack].rec_type == 2:
                    self.track[itrack].v_number = np.array(self.track[itrack].v_number, dtype=np.float32)

    def check_validity(self):
        r = []
        r.append(['Device','Port','Track','Type','Valid','Total','SamplingRate'])
        for itrack in self.track:
            if self.track[itrack].did != 0:
                device = self.device[self.track[itrack].did]
                if self.track[itrack].rec_type == 2:
                    recoding_type = 'Number'
                    valid = 0
                    for i in range(len(self.track[itrack].v_number)):
                        if self.track[itrack].v_number[i] >= self.track[itrack].minval and self.track[itrack].v_number[
                            i] <= self.track[itrack].maxval:
                            valid = valid + 1
                    result_track = [device.devname, device.port, self.track[itrack].name, recoding_type, valid,
                                    len(self.track[itrack].v_number), self.track[itrack].srate]
                    #                result_track.extend([self.track[itrack].minval, self.track[itrack].maxval])
                    r.append(result_track)
                elif self.track[itrack].rec_type == 1 or self.track[itrack].rec_type == 5 or self.track[
                    itrack].rec_type == 6:
                    recoding_type = 'Wave'
                    valid = 0
                    for i in range(len(self.track[itrack].v_wave)):
                        if self.track[itrack].v_wave[i] >= self.track[itrack].minval and self.track[itrack].v_wave[i] <= \
                                self.track[itrack].maxval:
                            valid = valid + 1
                    result_track = [device.devname, device.port, self.track[itrack].name, recoding_type, valid,
                                    len(self.track[itrack].v_wave), self.track[itrack].srate]
                    #                result_track.extend([self.track[itrack].minval, self.track[itrack].maxval])
                    r.append(result_track)
        return r