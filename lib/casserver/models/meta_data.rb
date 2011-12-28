# Esta clase contiene informacion de configuracion
# de la organizacion conectada.
# Se utiliza antes de cualquier conexion
# para saber a que base de datos y que recursos 
# adicionales tiene acceso la organizacion actual

class MetaData 
  # Servidor en el que se encuentra la base de datos de _metadata
  $DB_SERVER =  "sql02.lciberica.es"

  attr_reader :organization, :row, :services, :db_host
  
  # *db_host*: Es el servidor donde se encuentra la base de datos de metadata.
  #   Esto es util para ciertas tareas de rake que pueden ejecutarse en un 
  #   entorno con informacion de otro.
  #   Por defecto se conecta a $DB_SERVER.
  def initialize(organization, db_host = nil)
    @organization = organization
    @db_host = db_host
    
    # Si no se encuentra leerlo de la base de datos
    update_row_from_db
  end

  def update_row_from_db
    @row = read_from_db
    if @row
      initialize_vars @row
    else
      # Si no se encuentra en la bd tampoco marcarlo como 
      # desactivado
      @activo = false
      @services = Hash.new(0) # No tiene servicios activos
    end    
  end

  def initialize_vars row
    @services = YAML::load(row['services'])
    @activo = row['activo']
  end
  
  def activo
    return false unless @row
    @row['activo'] == '1' ? true : false
  end
  
  # Guardar el hash de servicios de nuevo
  def set_services services
    # Convertir los services a yml    
    sql = "UPDATE _metadata.org SET services='#{services.to_yaml}' WHERE nombre='#{@organization}'"
    RAILS_DEFAULT_LOGGER.debug sql
    self.class.db_connection(@db_host).query(sql)
    # Actualizar los datos
    update_row_from_db
  end

  # Devuelve el numero de la revision 
  def self.revision
    return File.open('REVISION'){|f|f.read}.strip if File.file?('REVISION') 
    nil
  end

  # Devuelve la lista de organizaciones que tienen pspadmin activos
  def self.orgs_con_pspadmin(metadata_host = nil)
    sql = "SELECT nombre FROM _metadata.org WHERE activo=1 AND pspadmin_mysql IS NOT NULL"
    res = db_connection(metadata_host || $DB_SERVER).query(sql)
    if block_given?  
      res.each_hash{|cole| yield(cole['nombre'])}
    else  
      coles=[]
      res.each_hash{|cole| coles << cole['nombre']}
      coles
    end  
  end    

  # Devuelve la lista de organizaciones que tienen psp activos
  def self.orgs_con_psp(metadata_host = $DB_SERVER)
    sql = "SELECT nombre FROM _metadata.org WHERE activo=1"
    res = db_connection(metadata_host || $DB_SERVER).query(sql)
    if block_given?  
      res.each_hash{|cole| yield(cole['nombre'])}
    else  
      coles=[]
      res.each_hash{|cole| coles << cole['nombre']}
      coles
    end  
  end    

  # Indicar que la organizacion especificada finalizo la sincronizacion 
  # de todos sus datos en la fecha pasada
  def self.set_pspadmin_date_fullsync(organization, time, db_host = nil)
    sql = "UPDATE org SET pspadmin_date_fullsync='#{time.to_s(:db)}' WHERE nombre='#{organization}'"
    db_connection(db_host).query(sql)
  end 

  # Devuelve una lista de nombres de organizaciones del mismo grupo
  # Ahora mismo se basa en la funcion grupo y en la relacion de prefijoss
  def organizaciones
    return unless grupo
    sql = "SELECT nombre FROM _metadata.org WHERE nombre LIKE '#{grupo}%' AND activo=1 AND psp_mysql IS NOT NULL"
    res = db_connection(@db_host).query(sql)
    coles=[]
    res.each_hash{|cole| coles << cole['nombre']}
    coles
  end

  # Devuelve el grupo al que pertenece una entidad basandose en el nombre
  # Se podrá en el futuro implementar mediante campos adicionales en _metadata
  def grupo
    # Separar organizacion de entidad
    if ( md = %r{([[:alpha:]]+)(\d*)}.match(@organization))
      md[1]
    else
      nil
    end  
  end
  
  # La siguiente funcion devuelve una conexion a la bd de pspadmin
  # Devuelve nil si no puede conectar
  def connection_pspadmin_db
    db_mysql = "admin_#{@organization}"
#    if ENV['RAILS_ENV'] == 'development'
#      ip_mysql = "127.0.0.1" 
#      user_mysql = "root"
#      pass_mysql = "charly"
#    else
    if @row
      ip_mysql = @row['pspadmin_mysql']
      pass_mysql = @row['pspadmin_mysql_pwd']
      user_mysql = "adm#{@organization}"
    end
    if ip_mysql
      RAILS_DEFAULT_LOGGER.info "Conectando con la BD de pspadmin('#{ip_mysql}', '#{user_mysql}', '#{db_mysql}')"
      puts "Conectando con la BD de pspadmin('#{ip_mysql}', '#{user_mysql}', '#{db_mysql}')"
      dbh = Mysql.real_connect(ip_mysql,user_mysql,pass_mysql,db_mysql)
      dbh.query("SET NAMES 'utf8'")
      dbh
    else
      nil
    end
  end

protected
  # Devuelve la conexion a la base de datos de metadatos
  # de organizacion
  def self.db_connection(metadata_host = nil)
    Mysql.real_connect(metadata_host || $DB_SERVER,
      "********",
      "********",
      "_metadata")
    rescue Mysql::Error => e
      return nil
  end

  # Lee el registro de esta organizacion desde la 
  # base de datos 
  def read_from_db
    sql = "SELECT * FROM _metadata.org WHERE nombre='#{@organization}'"
    con = self.class.db_connection(@db_host)
    if con
      res = con.query(sql)
      res.fetch_hash
    else
      nil
    end
  end
end
