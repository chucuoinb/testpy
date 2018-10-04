import base64
import requests
import pickle
import urllib.requests
import urllib.parse
import urllib
import json
import mysql.connector
import threading
import re
import string

from urllib.parse import urlparse


class File:
    _contentTypes = {'.art' : 'image/x-jg',
        '.bmp' : 'image/bmp',
        '.bmp_' : 'image/x-windows-bmp',
        '.dwg' : 'image/vnd.dwg',
        '.dwg_' : 'image/x-dwg',
        '.fif' : 'image/fif',
        '.flo' : 'image/florian',
        '.fpx' : 'image/vnd.fpx',
        '.fpx_' : 'image/vnd.net-fpx',
        '.g3' : 'image/g3fax',
        '.gif' : 'image/gif',
        '.ico' : 'image/x-icon',
        '.ief' : 'image/ief',
        '.jpg' : 'image/jpeg',
        '.jpeg' : 'image/pjpeg',
        '.jps' : 'image/x-jps',
        '.jut' : 'image/jutvision',
        '.mcf' : 'image/vasa',
        '.naplps' : 'image/naplps',
        '.niff' : 'image/x-niff',
        '.pbm' : 'image/x-portable-bitmap',
        '.pct' : 'image/x-pict',
        '.pcx' : 'image/x-pcx',
        '.pgm' : 'image/x-portable-graymap',
        '.pgm_' : 'image/x-portable-greymap',
        '.pict' : 'image/pict',
        '.xpm' : 'image/x-xpixmap',
        '.png' : 'image/png',
        '.pnm' : 'image/x-portable-anymap',
        '.ppm' : 'image/x-portable-pixmap',
        '.qif' : 'image/x-quicktime',
        '.rast' : 'image/cmu-raster',
        '.ras' : 'image/x-cmu-raster',
        '.rf' : 'image/vnd.rn-realflash',
        '.rgb' : 'image/x-rgb',
        '.rp' : 'image/vnd.rn-realpix',
        '.tif' : 'image/tiff',
        '.tiff' : 'image/x-tiff',
        '.wbmp' : 'image/vnd.wap.wbmp',
        '.xbm' : 'image/x-xbitmap',
        '.xbm_' : 'image/x-xbm',
        '.xbm__' : 'image/xbm',
        '.xif' : 'image/vnd.xiff',
        '.xpm_' : 'image/xpm',
        '.xwd' : 'image/x-xwd',
        '.xwd_' : 'image/x-xwindowdump',
        '.abc' : 'text/vnd.abc',
        '.htm' : 'text/html',
        '.aip' : 'text/x-audiosoft-intra',
        '.asm' : 'text/x-asm',
        '.asp' : 'text/asp',
        '.c' : 'text/x-c',
        '.txt' : 'text/plain',
        '.csh' : 'text/x-script.csh',
        '.css' : 'text/css',
        '.el' : 'text/x-script.elisp',
        '.etx' : 'text/x-setext',
        '.for' : 'text/x-fortran',
        '.flx' : 'text/vnd.fmi.flexstor',
        '.h' : 'text/x-h',
        '.hlb' : 'text/x-script',
        '.htc' : 'text/x-component',
        '.htt' : 'text/webviewhtml',
        '.ksh' : 'text/x-script.ksh',
        '.js' : 'text/javascript',
        '.js_' : 'text/ecmascript',
        '.java' : 'text/x-java-source',
        '.lsp' : 'text/x-script.lisp',
        '.lsx' : 'text/x-la-asf',
        '.m' : 'text/x-m',
        '.mcf_' : 'text/mcf',
        '.p' : 'text/x-pascal',
        '.pas' : 'text/pascal',
        '.pl' : 'text/x-script.perl',
        '.pm' : 'text/x-script.perl-module',
        '.py' : 'text/x-script.phyton',
        '.rexx' : 'text/x-script.rexx',
        '.rtx' : 'text/richtext',
        '.rt' : 'text/vnd.rn-realtext',
        '.scm' : 'text/x-script.guile',
        '.scm_' : 'text/x-script.scheme',
        '.sgm' : 'text/sgml',
        '.sgml' : 'text/x-sgml',
        '.sh' : 'text/x-script.sh',
        '.shtml' : 'text/x-server-parsed-html',
        '.spc' : 'text/x-speech',
        '.tcl' : 'text/x-script.tcl',
        '.tcsh' : 'text/x-script.tcsh',
        '.tsv' : 'text/tab-separated-values',
        '.uil' : 'text/x-uil',
        '.uri' : 'text/uri-list',
        '.uu' : 'text/x-uuencode',
        '.vcs' : 'text/x-vcalendar',
        '.wml' : 'text/vnd.wap.wml',
        '.wmls' : 'text/vnd.wap.wmlscript',
        '.wsc' : 'text/scriplet',
        '.xml' : 'text/xml',
        '.zsh' : 'text/x-script.zsh',
        '.aif' : 'audio/aiff',
        '.aiff' : 'audio/x-aiff',
        '.au' : 'audio/basic',
        '.au_' : 'audio/x-au',
        '.funk' : 'audio/make',
        '.gsm' : 'audio/x-gsm',
        '.it' : 'audio/it',
        '.jam' : 'audio/x-jam',
        '.midi' : 'audio/midi',
        '.la' : 'audio/nspaudio',
        '.lma' : 'audio/x-nspaudio',
        '.lam' : 'audio/x-liveaudio',
        '.mp2' : 'audio/mpeg',
        '.m3u' : 'audio/x-mpequrl',
        '.mid' : 'audio/x-mid',
        '.midi_' : 'audio/x-midi',
        '.mjf' : 'audio/x-vnd.audioexplosion.mjuicemediafile',
        '.mod' : 'audio/mod',
        '.mod_' : 'audio/x-mod',
        '.mp2_' : 'audio/x-mpeg',
        '.mp3' : 'audio/mpeg3',
        '.mp3_' : 'audio/x-mpeg-3',
        '.pfunk' : 'audio/make.my.funk',
        '.qcp' : 'audio/vnd.qcelp',
        '.rmp' : 'audio/x-pn-realaudio',
        '.ra' : 'audio/x-pn-realaudio-plugin',
        '.ra_' : 'audio/x-realaudio',
        '.rmi' : 'audio/mid',
        '.s3m' : 'audio/s3m',
        '.sid' : 'audio/x-psid',
        '.snd' : 'audio/x-adpcm',
        '.tsi' : 'audio/tsp-audio',
        '.tsp' : 'audio/tsplayer',
        '.voc' : 'audio/voc',
        '.voc_' : 'audio/x-voc',
        '.vox' : 'audio/voxware',
        '.vql' : 'audio/x-twinvq-plugin',
        '.vqf' : 'audio/x-twinvq',
        '.wav' : 'audio/wav',
        '.wav_' : 'audio/x-wav',
        '.xm' : 'audio/xm',
        '.afl' : 'video/animaflex',
        '.asf' : 'video/x-ms-asf',
        '.asx' : 'video/x-ms-asf-plugin',
        '.avi' : 'video/avi',
        '.avi_' : 'video/msvideo',
        '.avi__' : 'video/x-msvideo',
        '.avs' : 'video/avs-video',
        '.dv' : 'video/x-dv',
        '.dl' : 'video/dl',
        '.dl_' : 'video/x-dl',
        '.fli' : 'video/fli',
        '.fli_' : 'video/x-fli',
        '.fmf' : 'video/x-atomic3d-feature',
        '.gl' : 'video/gl',
        '.gl_' : 'video/x-gl',
        '.isu' : 'video/x-isvideo',
        '.m1v' : 'video/mpeg',
        '.mp3__' : 'video/mpeg',
        '.mjpg' : 'video/x-motion-jpeg',
        '.qt' : 'video/quicktime',
        '.movie' : 'video/x-sgi-movie',
        '.mp3___' : 'video/x-mpeg',
        '.mp2__' : 'video/x-mpeq2a',
        '.qtc' : 'video/x-qtc',
        '.rv' : 'video/vnd.rn-realvideo',
        '.scm__' : 'video/x-scm',
        '.vdo' : 'video/vdo',
        '.viv' : 'video/vivo',
        '.vivo' : 'video/vnd.vivo',
        '.vos' : 'video/vosaic',
        '.xdr' : 'video/x-amt-demorun',
        '.xsr' : 'video/x-amt-showrun',
        '' : 'application/octet-stream',
        '.aab' : 'application/x-authorware-bin',
        '.aam' : 'application/x-authorware-map',
        '.aas' : 'application/x-authorware-seg',
        '.ps' : 'application/postscript',
        '.aim' : 'application/x-aim',
        '.ani' : 'application/x-navi-animation',
        '.aos' : 'application/x-nokia-9000-communicator-add-on-software',
        '.aps' : 'application/mime',
        '.arj' : 'application/arj',
        '.asx_' : 'application/x-mplayer2',
        '.avi___' : 'application/x-troff-msvideo',
        '.bcpio' : 'application/x-bcpio',
        '.bin' : 'application/mac-binary',
        '.bin_' : 'application/macbinary',
        '.bin__' : 'application/x-binary',
        '.bin___' : 'application/x-macbinary',
        '.book' : 'application/book',
        '.bz2' : 'application/x-bzip2',
        '.bsh' : 'application/x-bsh',
        '.bz' : 'application/x-bzip',
        '.cat' : 'application/vnd.ms-pki.seccat',
        '.ccad' : 'application/clariscad',
        '.cco' : 'application/x-cocoa',
        '.cdf' : 'application/cdf',
        '.cdf_' : 'application/x-cdf',
        '.nc' : 'application/x-netcdf',
        '.cer' : 'application/pkix-cert',
        '.crt' : 'application/x-x509-ca-cert',
        '.chat' : 'application/x-chat',
        '.class' : 'application/java',
        '.class_' : 'application/java-byte-code',
        '.class__' : 'application/x-java-class',
        '.cpio' : 'application/x-cpio',
        '.cpt' : 'application/mac-compactpro',
        '.cpt_' : 'application/x-compactpro',
        '.cpt__' : 'application/x-cpt',
        '.crl' : 'application/pkcs-crl',
        '.crl_' : 'application/pkix-crl',
        '.crt_' : 'application/x-x509-user-cert',
        '.csh_' : 'application/x-csh',
        '.css_' : 'application/x-pointplus',
        '.dir' : 'application/x-director',
        '.deepv' : 'application/x-deepv',
        '.doc' : 'application/msword',
        '.dp' : 'application/commonground',
        '.drw' : 'application/drafting',
        '.dvi' : 'application/x-dvi',
        '.dwg__' : 'application/acad',
        '.dxf' : 'application/dxf',
        '.elc' : 'application/x-bytecode.elisp',
        '.elc_' : 'application/x-elc',
        '.env' : 'application/x-envoy',
        '.es' : 'application/x-esrehber',
        '.evy' : 'application/envoy',
        '.fdf' : 'application/vnd.fdf',
        '.fif_' : 'application/fractals',
        '.frl' : 'application/freeloader',
        '.gsp' : 'application/x-gsp',
        '.gss' : 'application/x-gss',
        '.gtar' : 'application/x-gtar',
        '.gz' : 'application/x-compressed',
        '.zip' : 'application/x-compressed',
        '.tgz' : 'application/x-compressed',
        '.gzip' : 'application/x-gzip',
        '.hdf' : 'application/x-hdf',
        '.help' : 'application/x-helpfile',
        '.hgl' : 'application/vnd.hp-hpgl',
        '.hlp' : 'application/hlp',
        '.hlp_' : 'application/x-winhelp',
        '.hqx' : 'application/binhex',
        '.hqx_' : 'application/binhex4',
        '.hqx__' : 'application/mac-binhex',
        '.hqx___' : 'application/mac-binhex40',
        '.hqx____' : 'application/x-binhex40',
        '.hqx_____' : 'application/x-mac-binhex40',
        '.hta' : 'application/hta',
        '.iges' : 'application/iges',
        '.ima' : 'application/x-ima',
        '.imap' : 'application/x-httpd-imap',
        '.inf' : 'application/inf',
        '.ins' : 'application/x-internett-signup',
        '.ip' : 'application/x-ip2',
        '.iv' : 'application/x-inventor',
        '.ivy' : 'application/x-livescreen',
        '.jcm' : 'application/x-java-commerce',
        '.js__' : 'application/x-javascript',
        '.js___' : 'application/javascript',
        '.js____' : 'application/ecmascript',
        '.ksh_' : 'application/x-ksh',
        '.latex' : 'application/x-latex',
        '.lha' : 'application/lha',
        '.lha_' : 'application/x-lha',
        '.lsp_' : 'application/x-lisp',
        '.lzh' : 'application/x-lzh',
        '.lzx' : 'application/lzx',
        '.lzx_' : 'application/x-lzx',
        '.man' : 'application/x-troff-man',
        '.map' : 'application/x-navimap',
        '.mbd' : 'application/mbedlet',
        '.mc' : 'application/x-magic-cap-package-1.0',
        '.mcd' : 'application/mcad',
        '.mcd_' : 'application/x-mathcad',
        '.mcp' : 'application/netmc',
        '.me' : 'application/x-troff-me',
        '.mif' : 'application/x-frame',
        '.mif_' : 'application/x-mif',
        '.mme' : 'application/base64',
        '.mm' : 'application/x-meme',
        '.mpc' : 'application/x-project',
        '.mpp' : 'application/vnd.ms-project',
        '.mrc' : 'application/marc',
        '.ms' : 'application/x-troff-ms',
        '.mzz' : 'application/x-vnd.audioexplosion.mzz',
        '.ncm' : 'application/vnd.nokia.configuration-message',
        '.nix' : 'application/x-mix-transfer',
        '.nsc' : 'application/x-conference',
        '.nvd' : 'application/x-navidoc',
        '.oda' : 'application/oda',
        '.omc' : 'application/x-omc',
        '.omcd' : 'application/x-omcdatamaker',
        '.omcr' : 'application/x-omcregerator',
        '.p10' : 'application/pkcs10',
        '.p10_' : 'application/x-pkcs10',
        '.p12' : 'application/pkcs-12',
        '.p12_' : 'application/x-pkcs12',
        '.p7a' : 'application/x-pkcs7-signature',
        '.p7m' : 'application/pkcs7-mime',
        '.p7c' : 'application/x-pkcs7-mime',
        '.p7r' : 'application/x-pkcs7-certreqresp',
        '.p7s' : 'application/pkcs7-signature',
        '.prt' : 'application/pro_eng',
        '.pcl' : 'application/vnd.hp-pcl',
        '.pcl_' : 'application/x-pcl',
        '.pdf' : 'application/pdf',
        '.pkg' : 'application/x-newton-compatible-pkg',
        '.pko' : 'application/vnd.ms-pki.pko',
        '.plx' : 'application/x-pixclscript',
        '.pm4' : 'application/x-pagemaker',
        '.pnm_' : 'application/x-portable-anymap',
        '.ppt' : 'application/mspowerpoint',
        '.pot' : 'application/vnd.ms-powerpoint',
        '.ppt_' : 'application/powerpoint',
        '.ppt__' : 'application/x-mspowerpoint',
        '.pre' : 'application/x-freelance',
        '.ras_' : 'application/x-cmu-raster',
        '.rm' : 'application/vnd.rn-realmedia',
        '.rng' : 'application/ringing-tones',
        '.rng_' : 'application/vnd.nokia.ringing-tone',
        '.rnx' : 'application/vnd.rn-realplayer',
        '.roff' : 'application/x-troff',
        '.rtf' : 'application/rtf',
        '.rtf_' : 'application/x-rtf',
        '.sbk' : 'application/x-tbook',
        '.scm___' : 'application/x-lotusscreencam',
        '.sdp' : 'application/sdp',
        '.sdp_' : 'application/x-sdp',
        '.sdr' : 'application/sounder',
        '.sea' : 'application/sea',
        '.sea_' : 'application/x-sea',
        '.set' : 'application/set',
        '.sh_' : 'application/x-sh',
        '.shar' : 'application/x-shar',
        '.sit' : 'application/x-sit',
        '.sit_' : 'application/x-stuffit',
        '.sl' : 'application/x-seelogo',
        '.smil' : 'application/smil',
        '.sol' : 'application/solids',
        '.spc_' : 'application/x-pkcs7-certificates',
        '.spl' : 'application/futuresplash',
        '.sprite' : 'application/x-sprite',
        '.wsrc' : 'application/x-wais-source',
        '.ssm' : 'application/streamingmedia',
        '.sst' : 'application/vnd.ms-pki.certstore',
        '.step' : 'application/step',
        '.stl' : 'application/sla',
        '.stl_' : 'application/vnd.ms-pki.stl',
        '.stl__' : 'application/x-navistyle',
        '.sv4cpio' : 'application/x-sv4cpio',
        '.sv4crc' : 'application/x-sv4crc',
        '.wrl' : 'application/x-world',
        '.swf' : 'application/x-shockwave-flash',
        '.tar' : 'application/x-tar',
        '.tbk' : 'application/toolbook',
        '.tcl_' : 'application/x-tcl',
        '.tex' : 'application/x-tex',
        '.texinfo' : 'application/x-texinfo',
        '.text' : 'application/plain',
        '.tgz_' : 'application/gnutar',
        '.tsp_' : 'application/dsptype',
        '.unv' : 'application/i-deas',
        '.ustar' : 'application/x-ustar',
        '.vcd' : 'application/x-cdlink',
        '.vda' : 'application/vda',
        '.vew' : 'application/groupwise',
        '.vmd' : 'application/vocaltec-media-desc',
        '.vmf' : 'application/vocaltec-media-file',
        '.vrml' : 'application/x-vrml',
        '.vsw' : 'application/x-visio',
        '.w60' : 'application/wordperfect6.0',
        '.w61' : 'application/wordperfect6.1',
        '.wb1' : 'application/x-qpro',
        '.web' : 'application/vnd.xara',
        '.wk1' : 'application/x-123',
        '.wmlc' : 'application/vnd.wap.wmlc',
        '.wmlsc' : 'application/vnd.wap.wmlscriptc',
        '.wp' : 'application/wordperfect',
        '.wpd' : 'application/x-wpwin',
        '.wq1' : 'application/x-lotus',
        '.wri' : 'application/mswrite',
        '.wri_' : 'application/x-wri',
        '.wtk' : 'application/x-wintalk',
        '.xls' : 'application/excel',
        '.xls_' : 'application/x-excel',
        '.xls__' : 'application/x-msexcel',
        '.xls___' : 'application/vnd.ms-excel',
        '.xml____' : 'application/xml',
        '.xpix' : 'application/x-vnd.ls-xpix',
        '.zip__' : 'application/x-compress',
        '.zip_' : 'application/zip',
        '.mid_' : 'application/x-midi',
        '.3dmf' : 'x-world/x-3dmf',
        '.mime' : 'message/rfc822',
        '.mime_' : 'www/mime',
        '.pdb' : 'chemical/x-pdb',
        '.pov' : 'model/x-pov',
        '.pvu' : 'paleovu/x-pv',
        '.pyc' : 'applicaiton/x-bytecode.python',
        '.svr' : 'x-world/x-svr',
        '.ustar_' : 'multipart/x-ustar',
        '.vrml_' : 'model/vrml',
        '.wrz' : 'x-world/x-vrml',
        '.vrt' : 'x-world/x-vrt',
        '.xgz' : 'xgl/drawing',
        '.wmf' : 'windows/metafile',
        '.xmz' : 'xgl/movie',
        '.zip___' : 'multipart/x-zip',
        '.dwf' : 'drawing/x-dwf',
        '.dwf_' : 'model/vnd.dwf',
        '.gzip_' : 'multipart/x-gzip',
        '.ice' : 'x-conference/x-cooltalk',
        '.iges_' : 'model/iges',
        '.ivr' : 'i-world/i-vrml',
        '.kar' : 'music/x-karaoke',
        '.mhtml' : 'message/rfc822',
        '.midi__' : 'music/crescendo',
        '.midi___' : 'x-music/x-midi'}


    def stripDomainFromUrl(self,url):
        path = parse(url, PHP_URL_PATH)
        query = parse(url, PHP_URL_QUERY)
        fragment = parse(url, PHP_URL_FRAGMENT)
        if(query):
            path = path + '?' + query
        if(fragment):
            path = path + '#' + fragment
        return path

    def joinUrlPath(self,url, path):
        full_url = url.rstrip('/')
        if(path):
            full_url = full_url + '/' + path.lstrip('/')
        return full_url


    def isUrlEncode(self,path):
        is_encoded = re.search('~%[0-9A-F]{2}~i', path)
        return is_encoded

    
    def getRawUrl(self,url):
        scheme = urlparse(url, PHP_URL_SCHEME)
        user = urlparse(url, PHP_URL_USER)
        passs = urlparse(url, PHP_URL_PASS)
        host = urlparse(url, PHP_URL_HOST)
        port = urlparse(url, PHP_URL_PORT)
        path = urlparse(url, PHP_URL_PATH)
        query = urlparse(url, PHP_URL_QUERY)
        fragment = urlparse(url, PHP_URL_FRAGMENT)
        raw_url = ''
        if(scheme) raw_url = raw_url + scheme + '://'
        if(user && path):
            raw_url =  raw_url + user + ':'+ passs + '@'
        raw_url = raw_url + host
        if(port) raw_url = raw_url +  ':' + port
        if(self.isUrlEncode(path)):
            raw_url = raw_url + path
        else:
            raw_url = raw_url + self.rawUrlEncode(path)
        if(query):
            raw_url =  raw_url + '?' + query
        if(fragment):
            raw_url = raw_url + '#' + fragment
        return raw_url

    def rawUrlEncode(self,path):
        splits = str.split(path,'/')
        raw_path = {}
        for key,split in splits.items():
            raw_path[key] = rawurlencode(split)
        raw_path = str.join('/', raw_path)
        return raw_path

    def isVirtualUrl(self,url, path = ''):
        result = False
        full_url = url
        
        if(path):
            full_url = self.joinUrlPath(url, path)
        query = urlparse(full_url, PHP_URL_QUERY)

        if(query):
            result = True
        return result

    def getFileNameFromVirtualUrl(self,url, path = '', join_query =  True):
        fileName = None
        respone = urllib.request.urlopen(url)
        headers = respone.getheaders()
        headers = dict(zip((map("strtolower",headers.keys()),headers)))
        if('content-disposition' in headers):
            pos = headers['content-disposition'].find('=')
            if pos >= 0:
                fileName = headers['content-disposition'][pos:]
                fileName = filename.strip("=\"'")

        if(fileName == False):
            extension = self.getFileTypeByContentType(headers['content-type'])
            if(path == False):
                path = self.stripDomainFromUrl(url)
                fileName = urlparse(path, PHP_URL_PATH)
            if(join_query):
                fileName = self.joinQueryToFileName(path)

            fileName = filename +  extension

        return fileName

    def getFileTypeByContentType(self,content_type):
        file_type = self._contentTypes.index(content_type)
        file_type = file_type.replace('_','')
        return file_type


    def joinQueryToFileName(self,path):
        base_name = urlparse(path, PHP_URL_PATH)
        query = urlparse(path, PHP_URL_QUERY)
        if(query):
            base_name = base_name +  '-' + query
        return base_name

    def changeSpecialCharInPath(self,path, character = '-'):
        splits = str.split(path,'/')
        data = {}
        for key,split in splits.items():
            split = split.replace('/[^A-Za-z0-9.\-_]/', character)
            data[key] = split
        path = str.join('/',data)
        return path




















